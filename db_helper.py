import psycopg2

class Db:
    def __init__(self, url):
        self.url = url

    CREATE_CONTACT_TABLE = ("CREATE TABLE IF NOT EXISTS CONTACT ("
                            "id SERIAL PRIMARY KEY,"
                            "phoneNumber VARCHAR(20),"
                            "phoneNumber VARCHAR(50),"
                            "linkedId INT,"
                            "linkPrecedence VARCHAR(10),"
                            "createdAt TIMESTAMP,"
                            "updatedAt TIMESTAMP,"
                            "deletedAt TIMESTAMP"                        
                            ");")

    GET_MATCHING_ROWS = ("SELECT id, phoneNumber, email, linkedId FROM CONTACT "
                         "WHERE phoneNumber = %s or email = %s ORDER BY createdAt;")

    CREATE_ROW = (
        "INSERT INTO CONTACT (phoneNumber, email, linkedId, linkPrecedence, createdAt, updatedAt, deletedAt)"
        "VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL) RETURNING id;")

    SELECT_OLDEST_FOR_UPDATE = "SELECT id FROM CONTACT WHERE id = Any(%s) or linkedId = Any(%s) ORDER BY createdAt;"

    UPDATE_ROW = ("UPDATE CONTACT SET linkedId = %s, linkPrecedence = 'secondary', updatedAt = Now()"
                  "where id = Any(%s) RETURNING id;")

    SELECT_RELATED = ("SELECT id, phoneNumber, email, linkedId FROM CONTACT WHERE id = %s or linkedId = %s"
                      " ORDER BY createdAt ;")

    def create_table(self):
        conn = psycopg2.connect(self.url)
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(Db.CREATE_CONTACT_TABLE)
        return True

    def get_matching_rows(self, ph_num, email):
        conn = psycopg2.connect(self.url)
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(Db.GET_MATCHING_ROWS, (ph_num, email))
                rows = cursor.fetchall()
        return rows

    def create_record(self, ph_num, email, primary_id, link_precedence):
        conn = psycopg2.connect(self.url)
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(Db.CREATE_ROW, (ph_num, email, primary_id, link_precedence))
                rec_id = cursor.fetchone()[0]
        return rec_id

    def update_record(self, oldest_id: int, related_ids: [int]):
        conn = psycopg2.connect(self.url)
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(Db.UPDATE_ROW, (oldest_id, related_ids, ))
                rows = cursor.fetchall()
        return rows

    def get_oldest_rec(self, related_ids: [int]):
        conn = psycopg2.connect(self.url)
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(Db.SELECT_OLDEST_FOR_UPDATE, (related_ids, related_ids, ))
                rows = cursor.fetchall()
        return rows

    def select_related(self, primary_id):
        conn = psycopg2.connect(self.url)
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(Db.SELECT_RELATED, (primary_id, primary_id))
                rows = cursor.fetchall()
        return rows
