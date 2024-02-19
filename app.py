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
    data = request.get_json()
    result = process_data(data)
    return result

def process_data(data: {str: str}):
    primary_id, link_precedence = None, 'primary'

    if data["phoneNumber"] or data["email"]:
        new_info = True
        related_ids = set()
        rows = db.get_matching_rows(data["phoneNumber"], data["email"])

        for rec_id, ph_num, email, linked_id in rows:
            if not primary_id:
                primary_id = linked_id if linked_id else rec_id
                link_precedence = 'secondary'
                related_ids.add(primary_id)
            if data["phoneNumber"] == ph_num and data["email"] == email:
                new_info = False
                break
            if linked_id:
                related_ids.add(linked_id)
            related_ids.add(rec_id)

        if new_info:
            primary_id = upsert_record(data, primary_id, link_precedence, related_ids)

    return get_result_records(primary_id)


def upsert_record(data, primary_id, link_precedence, related_ids):
    if data["phoneNumber"] and data["email"]:
        rows = db.get_oldest_rec(list(related_ids))
        oldest_rec = None

        for rec_id in rows:
            if not oldest_rec:
                oldest_rec = rec_id
                if oldest_rec in related_ids:
                    related_ids.remove(oldest_rec)
                continue
            related_ids.add(rec_id)
        db.update_record(oldest_rec, list(related_ids))
        primary_id = oldest_rec
    else:
        new_rec_id = db.create_record(data["phoneNumber"], data["email"], primary_id, link_precedence)
        if not primary_id:
            primary_id = new_rec_id
    return primary_id

def get_result_records(primary_id):
    emails, ph_nums, sec_ids = [], [], []
    if primary_id:
        rows = db.select_related(primary_id)
        first = False
        for r_id, r_phone, r_email, r_link in rows:
            if not first:
                primary_id = r_id
                first = True
            if r_phone not in ph_nums:
                ph_nums.append(r_phone)
            if r_email not in emails:
                emails.append(r_email)
            if r_link not in sec_ids:
                sec_ids.append(r_link)

    return build_result(primary_id, emails, ph_nums, sec_ids)


def build_result(primary_id, emails, ph_nums, sec_ids):
    return {
        "contact":
            {
                "primaryContactId": primary_id,
                "emails": emails,
                "phoneNumbers": ph_nums,
                "secondaryContactIds": sec_ids
            }
    }, 201
