import unittest

class TestCase(unittest.TestCase):
    def test_1(self):
        # Mimir Test Case begins here

        from Project03.project import connect
        conn = connect("test.db")
        def check(sql_statement, expected):
          print("SQL: " + sql_statement)
          result = conn.execute(sql_statement)
          result_list = list(result)

          print("expected:")
          print(expected)
          print("student: ")
          print(result_list)
          assert expected == result_list

        conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
        conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness'), (5345.6, 42, 'sadness'), (43.24, 25, 'life');")
        conn.execute("INSERT INTO table VALUES (323.4, 433, 'warmth'), (5.6, 42, 'thirst'), (4.4, 235, 'Skyrim');")
        conn.execute("INSERT INTO table VALUES (NULL, NULL, 'other'), (5.6, NULL, 'hunger'), (NULL, 235, 'want');")


        check(
          "SELECT * FROM table ORDER BY three;",
        [(4.4, 235, 'Skyrim'),
         (3.4, 43, 'happiness'),
         (5.6, None, 'hunger'),
         (43.24, 25, 'life'),
         (None, None, 'other'),
         (5345.6, 42, 'sadness'),
         (5.6, 42, 'thirst'),
         (None, 235, 'want'),
         (323.4, 433, 'warmth')]
          )

        conn.execute("DELETE FROM table WHERE one IS NULL;")

        check(
          "SELECT * FROM table ORDER BY three;",
        [(4.4, 235, 'Skyrim'),
         (3.4, 43, 'happiness'),
         (5.6, None, 'hunger'),
         (43.24, 25, 'life'),
         (5345.6, 42, 'sadness'),
         (5.6, 42, 'thirst'),
         (323.4, 433, 'warmth')]
          )

        conn.execute("DELETE FROM table WHERE two < 50;")


        check(
          "SELECT * FROM table ORDER BY three;",
          [(4.4, 235, 'Skyrim'), (5.6, None, 'hunger'), (323.4, 433, 'warmth')]

        )
if __name__ == "__main__":
    unittest.main()
