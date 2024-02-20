CREATE_CONTACT_TABLE = ("CREATE TABLE IF NOT EXISTS CONTACT ("
                        "id SERIAL PRIMARY KEY,"
                        "phoneNumber VARCHAR(20),"
                        "email VARCHAR(50),"
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
