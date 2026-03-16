import os
import json
import csv
import logging
from datetime import datetime
import boto3
import snowflake.connector
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from slugify import slugify

# ---------------------------------------------------
# Logging Configuration
# ---------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------
# AWS Clients
# ---------------------------------------------------
s3 = boto3.client("s3")

# ---------------------------------------------------
# Environment Variables
# ---------------------------------------------------
SNOWFLAKE_CONFIG = {
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
    "database": os.environ.get("SNOWFLAKE_DATABASE"),
    "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
}

ELASTIC_CONFIG = {
    "host": os.environ.get("ELASTIC_HOST"),
    "username": os.environ.get("ELASTIC_USERNAME"),
    "password": os.environ.get("ELASTIC_PASSWORD"),
    "index": os.environ.get("ELASTIC_INDEX", "transactions")
}

# ---------------------------------------------------
# Expected Columns from CSV
# ---------------------------------------------------
EXPECTED_COLUMNS = [
    "propertyStatus", "price", "numberOfBeds", "numberOfBaths", "sqft",
    "addr1", "addr2", "streetNumber", "streetName", "streetType",
    "preDirection", "unitType", "unitNumber", "city", "state", "zipcode",
    "latitude", "longitude", "compassPropertyId", "propertyType",
    "yearBuilt", "presentedBy", "brokeredBy", "realtorMobile",
    "sourcePropertyId", "list_date", "pending_date", "openHouse",
    "listing_office_id", "listing_agent_id", "email", "pageLink", "scraped_date"
]

COLUMN_MAPPING = {
    "propertyStatus": "status",
    "numberOfBeds": "bedrooms",
    "numberOfBaths": "bathrooms",
    "sqft": "square_feet",
    "addr1": "address_line_1",
    "addr2": "address_line_2",
    "streetNumber": "street_number",
    "streetName": "street_name",
    "streetType": "street_type",
    "preDirection": "pre_direction",
    "unitType": "unit_type",
    "unitNumber": "unit_number",
    "zipcode": "zip_code",
    "compassPropertyId": "compass_property_id",
    "propertyType": "property_type",
    "presentedBy": "presented_by",
    "brokeredBy": "brokered_by",
    "realtorMobile": "realtor_mobile",
    "sourcePropertyId": "source_property_id",
    "openHouse": "open_house_json",           # ← updated name for clarity
    "pageLink": "page_link"
}

STATUS_MAPPING = {
    "Active Under Contract": "Pending",
    "New": "Active",
    "Closed": "Sold"
}

# ---------------------------------------------------
# Helper Functions
# ---------------------------------------------------
def get_first_scalar(value, field=""):
    if value is None:
        return None
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.strip():
                return item.strip()
        return None
    if isinstance(value, str):
        return value.strip()
    return None

def clean_phone(phone_str):
    phone = get_first_scalar(phone_str)
    if not phone:
        return None
    digits = "".join(c for c in phone if c.isdigit())
    return digits[:10] if len(digits) >= 10 else None

def parse_name(full_name):
    name = get_first_scalar(full_name)
    if not name:
        return None, None, None
    parts = name.split()
    if len(parts) == 0:
        return None, None, None
    first = parts[0]
    last = parts[-1] if len(parts) > 1 else None
    middle = " ".join(parts[1:-1]) if len(parts) > 2 else None
    return first, middle, last

def parse_open_house(raw_oh):
    """
    Parse Compass-style open house JSON.
    Returns (start_time_millis, company, contact_name) or (None, None, None)
    Handles list-of-dicts (takes first), nested 'contact' object, etc.
    """
    if not raw_oh or not isinstance(raw_oh, str):
        return None, None, None

    raw = raw_oh.strip()
    if raw in ('', '[]', '{}', 'null', '""', "''"):
        return None, None, None

    try:
        parsed = json.loads(raw)

        # Normalize to dict (take first item if list)
        if isinstance(parsed, list):
            if not parsed or not isinstance(parsed[0], dict):
                return None, None, None
            data = parsed[0]
        elif isinstance(parsed, dict):
            data = parsed
        else:
            return None, None, None

        # Extract fields from real structure
        start_time = data.get("startTimeMillis")
        contact = data.get("contact", {})
        company = contact.get("company")
        contact_name = contact.get("contactName")

        return start_time, company, contact_name

    except json.JSONDecodeError as e:
        logger.warning(f"Invalid open house JSON: {raw[:150]}... | error: {str(e)}")
        return None, None, None
    except Exception as e:
        logger.error(f"Unexpected error parsing open house: {str(e)}", exc_info=True)
        return None, None, None

def build_full_address(data):
    parts = [
        data.get("address_line_1"),
        data.get("address_line_2"),
        data.get("city"),
        data.get("state"),
        data.get("zip_code")
    ]
    return ", ".join(p.strip() for p in parts if p and p.strip())

def safe_parse_date(date_str, field_name="date"):
    if not date_str or not isinstance(date_str, str):
        return None
    date_str = date_str.strip()
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%m/%d/%Y")
        return dt.date().isoformat()
    except ValueError:
        try:
            dt = datetime.strptime(date_str, "%m/%d/%y")
            return dt.date().isoformat()
        except ValueError:
            logger.warning(f"Invalid date format for {field_name}: '{date_str}'")
            return None

# ---------------------------------------------------
# Lambda Handler
# ---------------------------------------------------
def lambda_handler(event, context):
    try:
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]
        logger.info(f"Processing file: s3://{bucket}/{key}")

        obj = s3.get_object(Bucket=bucket, Key=key)
        lines = obj["Body"].read().decode("utf-8").splitlines()
        reader = csv.DictReader(lines)

        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        es = Elasticsearch(
            hosts=[ELASTIC_CONFIG["host"]],
            basic_auth=(ELASTIC_CONFIG["username"], ELASTIC_CONFIG["password"]),
            request_timeout=60
        )

        processed = 0
        failed = 0
        BATCH_SIZE = 120
        snowflake_batch = []
        es_actions = []

        for row in reader:
            try:
                # Clean and map row
                cleaned_row = {k.strip(): v.strip() if isinstance(v, str) else v 
                              for k, v in row.items() if k in EXPECTED_COLUMNS}
                data = {COLUMN_MAPPING.get(k, k): get_first_scalar(v) 
                       for k, v in cleaned_row.items()}

                status = STATUS_MAPPING.get(data.get("status"), data.get("status", "Unknown"))

                first, middle, last = parse_name(data.get("presented_by"))

                # ← FIXED parsing
                oh_start, oh_company, oh_contact = parse_open_house(data.get("open_house_json"))

                emails = [e.strip() for e in (data.get("email") or "").split(",") if e.strip()]
                email1 = emails[0] if emails else None
                email2 = emails[1] if len(emails) > 1 else None

                phone_clean = clean_phone(data.get("realtor_mobile"))

                full_address = build_full_address(data)

                list_d   = safe_parse_date(data.get("list_date"),    "list_date")
                pending_d = safe_parse_date(data.get("pending_date"), "pending_date")
                scraped_d = safe_parse_date(data.get("scraped_date"), "scraped_date")

                # Generate ID
                mls = data.get("source_property_id")
                addr = data.get("address_line_1")
                city_val = data.get("city")
                state_val = data.get("state")
                zip_val = data.get("zip_code")

                id_parts = [p for p in [mls, addr, city_val, state_val, zip_val] if p]
                id_str = "-".join(id_parts)
                record_id = slugify(id_str) if id_str else f"unknown-{processed}"

                if not record_id or record_id == "unknown":
                    logger.warning(f"Skipping row - no meaningful ID: {mls or 'no-mls'}")
                    failed += 1
                    continue

                record = {
                    "id": record_id,
                    "status": status,
                    "price": data.get("price"),
                    "bedrooms": data.get("bedrooms"),
                    "bathrooms": data.get("bathrooms"),
                    "square_feet": data.get("square_feet"),
                    "full_address": full_address,
                    "address_line_1": data.get("address_line_1"),
                    "address_line_2": data.get("address_line_2"),
                    "city": data.get("city"),
                    "state": data.get("state"),
                    "zip_code": data.get("zip_code"),
                    "street_number": data.get("street_number"),
                    "street_name": data.get("street_name"),
                    "street_type": data.get("street_type"),
                    "pre_direction": data.get("pre_direction"),
                    "unit_type": data.get("unit_type"),
                    "unit_number": data.get("unit_number"),
                    "latitude": data.get("latitude"),
                    "longitude": data.get("longitude"),
                    "compass_property_id": data.get("compass_property_id"),
                    "property_type": data.get("property_type"),
                    "presented_by": data.get("presented_by"),
                    "presented_by_first_name": first,
                    "presented_by_middle_name": middle,
                    "presented_by_last_name": last,
                    "realtor_mobile": data.get("realtor_mobile"),
                    "phone": phone_clean,
                    "email_1": email1,
                    "email_2": email2,
                    "oh_start_time": oh_start,          # now gets millis timestamp
                    "oh_company": oh_company,
                    "oh_contact_name": oh_contact,
                    "list_date": list_d,
                    "pending_date": pending_d,
                    "scraped_date": scraped_d,
                    "brokered_by": data.get("brokered_by"),
                    "source_property_id": data.get("source_property_id"),
                    "page_link": data.get("page_link"),
                    "listing_office_id": data.get("listing_office_id"),
                    "listing_agent_id": data.get("listing_agent_id")
                }

                snowflake_batch.append(record)
                es_actions.append({
                    "_index": ELASTIC_CONFIG["index"],
                    "_id": record_id,
                    "_source": record
                })

                # Batch flush
                if len(snowflake_batch) >= BATCH_SIZE:
                    cursor.executemany("""
                        INSERT INTO transactions (
                            id, status, price, bedrooms, bathrooms, square_feet,
                            full_address, address_line_1, address_line_2, city, state, zip_code,
                            street_number, street_name, street_type, pre_direction,
                            unit_type, unit_number, latitude, longitude,
                            compass_property_id, property_type,
                            presented_by, presented_by_first_name, presented_by_middle_name, presented_by_last_name,
                            realtor_mobile, phone, email_1, email_2,
                            oh_start_time, oh_company, oh_contact_name,
                            list_date, pending_date, scraped_date,
                            brokered_by, source_property_id, page_link,
                            listing_office_id, listing_agent_id
                        ) VALUES (
                            %(id)s, %(status)s, %(price)s, %(bedrooms)s, %(bathrooms)s, %(square_feet)s,
                            %(full_address)s, %(address_line_1)s, %(address_line_2)s, %(city)s, %(state)s, %(zip_code)s,
                            %(street_number)s, %(street_name)s, %(street_type)s, %(pre_direction)s,
                            %(unit_type)s, %(unit_number)s, %(latitude)s, %(longitude)s,
                            %(compass_property_id)s, %(property_type)s,
                            %(presented_by)s, %(presented_by_first_name)s, %(presented_by_middle_name)s, %(presented_by_last_name)s,
                            %(realtor_mobile)s, %(phone)s, %(email_1)s, %(email_2)s,
                            %(oh_start_time)s, %(oh_company)s, %(oh_contact_name)s,
                            %(list_date)s, %(pending_date)s, %(scraped_date)s,
                            %(brokered_by)s, %(source_property_id)s, %(page_link)s,
                            %(listing_office_id)s, %(listing_agent_id)s
                        )
                    """, snowflake_batch)
                    snowflake_batch = []

                if len(es_actions) >= BATCH_SIZE * 2:
                    bulk(es, es_actions, raise_on_error=False, stats_only=True)
                    es_actions = []

                processed += 1

            except Exception as row_error:
                failed += 1
                logger.error(f"Row processing failed: {row_error}", exc_info=True)
                continue

        # Final flush
        if snowflake_batch:
            cursor.executemany("""
                INSERT INTO transactions (
                    id, status, price, bedrooms, bathrooms, square_feet,
                    full_address, address_line_1, address_line_2, city, state, zip_code,
                    street_number, street_name, street_type, pre_direction,
                    unit_type, unit_number, latitude, longitude,
                    compass_property_id, property_type,
                    presented_by, presented_by_first_name, presented_by_middle_name, presented_by_last_name,
                    realtor_mobile, phone, email_1, email_2,
                    oh_start_time, oh_company, oh_contact_name,
                    list_date, pending_date, scraped_date,
                    brokered_by, source_property_id, page_link,
                    listing_office_id, listing_agent_id
                ) VALUES (
                    %(id)s, %(status)s, %(price)s, %(bedrooms)s, %(bathrooms)s, %(square_feet)s,
                    %(full_address)s, %(address_line_1)s, %(address_line_2)s, %(city)s, %(state)s, %(zip_code)s,
                    %(street_number)s, %(street_name)s, %(street_type)s, %(pre_direction)s,
                    %(unit_type)s, %(unit_number)s, %(latitude)s, %(longitude)s,
                    %(compass_property_id)s, %(property_type)s,
                    %(presented_by)s, %(presented_by_first_name)s, %(presented_by_middle_name)s, %(presented_by_last_name)s,
                    %(realtor_mobile)s, %(phone)s, %(email_1)s, %(email_2)s,
                    %(oh_start_time)s, %(oh_company)s, %(oh_contact_name)s,
                    %(list_date)s, %(pending_date)s, %(scraped_date)s,
                    %(brokered_by)s, %(source_property_id)s, %(page_link)s,
                    %(listing_office_id)s, %(listing_agent_id)s
                )
            """, snowflake_batch)

        if es_actions:
            bulk(es, es_actions, raise_on_error=False, stats_only=True)

        cursor.close()
        conn.close()

        logger.info(f"Processing complete | rows: {processed} | failed: {failed}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Pipeline finished",
                "processed": processed,
                "failed": failed
            })
        }

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }