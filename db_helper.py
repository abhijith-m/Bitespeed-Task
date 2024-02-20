import psycopg2
import constants
import logging

logging.basicConfig(filename="appLog.log")

class Db:
    def __init__(self, url):
        self.url = url
        self.create_table()

    def create_table(self):
        try:
            with psycopg2.connect(self.url) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(constants.CREATE_CONTACT_TABLE)
        except Exception as e:
            logging.error(e)

    def get_matching_rows(self, ph_num, email):
        try:
            with psycopg2.connect(self.url) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(constants.GET_MATCHING_ROWS, (ph_num, email))
                    rows = cursor.fetchall()
            return rows
        except Exception as e:
            logging.error(e)

    def create_record(self, ph_num, email, primary_id, link_precedence):
        try:
            with psycopg2.connect(self.url) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(constants.CREATE_ROW, (ph_num, email, primary_id, link_precedence))
                    rec_id = cursor.fetchone()[0]
            return rec_id
        except Exception as e:
            logging.error(e)

    def update_record(self, oldest_id: int, related_ids: [int]):
        try:
            with psycopg2.connect(self.url) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(constants.UPDATE_ROW, (oldest_id, related_ids, ))
                    rows = cursor.fetchall()
            return rows
        except Exception as e:
            logging.error(e)

    def get_oldest_rec(self, related_ids: [int]):
        try:
            with psycopg2.connect(self.url) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(constants.SELECT_OLDEST_FOR_UPDATE, (related_ids, related_ids, ))
                    rows = cursor.fetchall()
            return rows
        except Exception as e:
            logging.error(e)

    def select_related(self, primary_id):
        try:
            with psycopg2.connect(self.url) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(constants.SELECT_RELATED, (primary_id, primary_id))
                    rows = cursor.fetchall()
            return rows if rows else []
        except Exception as e:
            logging.error(e)
