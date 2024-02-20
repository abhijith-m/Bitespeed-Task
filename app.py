import os
from dotenv import load_dotenv
import logging
from flask import Flask, request
from db_helper import Db

app = Flask(__name__)

load_dotenv()
db = Db(os.getenv("DATABASE_URL"))


@app.route('/identify', methods=['POST'])
def identify():
    request_data = request.get_json()
    result = process_identity_request(request_data)
    return result

def process_identity_request(request_data):
    """
    This function identifies all the rows that can be updated by matching it with phoneNumber OR email,
    Gets 1st set of related records (secondary and their primaries)
    Decides if a new record has to be created or an existing record(s) needs to be modified
    :param request_data:
    :return: result json with 200 status code
    """
    if not (request_data["phoneNumber"] or request_data["email"]):
        """
        Case 1: Bad request
        """
        return get_result_records(None)

    rows = db.get_matching_rows(request_data["phoneNumber"], request_data["email"])
    if len(rows) == 0:
        """
        Case 2: Fresh record, create new row
        """
        primary_id = create_record(request_data, None, 'primary')
        return get_result_records(primary_id)

    if not (request_data["phoneNumber"] and request_data["email"]):
        """
        Case 3: No new Information
        Request with just EXISTING email or phoneNumber
        Get the primary record id of the oldest record
        """
        for rec_id, ph_num, email, linked_id in rows:
            primary_id = linked_id if linked_id else rec_id
            return get_result_records(primary_id)

    primary_id= None
    new_info = True
    related_ids = set()
    email_exists, phone_exists = False, False
    for rec_id, ph_num, email, linked_id in rows:
        if not primary_id:
            primary_id = linked_id if linked_id else rec_id
            related_ids.add(primary_id)

        if request_data["phoneNumber"] == ph_num and request_data["email"] == email:
            """
            Case 4(a) : No new Information
            Request with just EXISTING email and phoneNumber -> Nothing to update
            """
            new_info = False
            break

        if linked_id:
            related_ids.add(linked_id)
        related_ids.add(rec_id)

        if ph_num == request_data["phoneNumber"]:
            phone_exists = True
        if email == request_data["email"]:
            email_exists = True

    if new_info:
        link_records = phone_exists and email_exists
        if link_records:
            """
            Case 4(b): Link two different records that are already present
            """
            primary_id = update_record(related_ids)
        else:
            """
            Case 4(c): Create new record and link it to oldest related primary record
            """
            primary_id = create_record(request_data, primary_id, 'secondary')

    return get_result_records(primary_id)


def update_record(related_ids):
    """
    Gets 2nd set of related records that could've been missed in the calling fn,
    A new record is created or link of a record is updated
    :param link_records:
    :param request_data:
    :param primary_id:
    :param link_precedence:
    :param related_ids:
    :return: id of the primary record
    """
    rows = db.get_oldest_rec(list(related_ids))
    oldest_rec = None

    for (rec_id,) in rows:
        if not oldest_rec:
            oldest_rec = rec_id
            if oldest_rec in related_ids:
                related_ids.remove(oldest_rec)
            continue
        related_ids.add(rec_id)
    db.update_record(oldest_rec, list(related_ids))
    primary_id = oldest_rec
    return primary_id

def create_record(request_data, primary_id, link_precedence):
    new_rec_id = db.create_record(request_data["phoneNumber"], request_data["email"], primary_id, link_precedence)
    if not primary_id:
        primary_id = new_rec_id
    return primary_id


def get_result_records(primary_id):
    """
    Get all related records based on newly created or recently updated row for building result
    :param primary_id:
    :return:
    """
    emails, ph_nums, sec_ids = [], [], []
    if primary_id:
        rows = db.select_related(primary_id)
        first = False
        for r_id, r_phone, r_email, r_link in rows:
            if not first:
                primary_id = r_id
                first = True
            if r_phone and r_phone not in ph_nums:
                ph_nums.append(r_phone)
            if r_email and r_email not in emails:
                emails.append(r_email)
            if r_link:
                sec_ids.append(r_id)

    return build_result(primary_id, emails, ph_nums, sec_ids)


def build_result(primary_id, emails, ph_nums, sec_ids):
    status_code = 200 if primary_id else 400
    return {
        "contact":
            {
                "primaryContactId": primary_id,
                "emails": emails,
                "phoneNumbers": ph_nums,
                "secondaryContactIds": sec_ids
            }
    }, status_code
