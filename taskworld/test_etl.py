import unittest
import psycopg2
from main import etl

class TestETL(unittest.TestCase):
    
    def setUp(self):
        self.source_file = './data/activity.csv'
        self.database = 'warehouse'
        self.table = 'user_activity'
        self.conn = psycopg2.connect(dbname=self.database, user="postgres", password="postgres", host="postgres", port=5432)
        self.cursor = self.conn.cursor()
        
    def tearDown(self):
        self.cursor.close()
        self.conn.close()
        
    def test_etl(self):
        etl(self.source_file, self.database, self.table)
        
        # Query the database for user_id = '5bfd0e8d472bcf0009a1014d' and assert the values for longest_streak and top_workspace
        self.cursor.execute(f"SELECT top_workspace, longest_streak FROM {self.table} WHERE user_id = '5bfd0e8d472bcf0009a1014d'")
        top_workspace, longest_streak = self.cursor.fetchone()
        
        self.assertEqual(top_workspace, 'EXPECTED_TOP_WORKSPACE')
        self.assertEqual(longest_streak, 'EXPECTED_LONGEST_STREAK')

if __name__ == '__main__':
    unittest.main()
