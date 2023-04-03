class DBWriter():
    def __init__(self, df, db_conn, ds):
        self.df = df
        self.db_conn = db_conn
        self.ds = ds

    def write_to_postgres(self):
        self.df.to_sql(
            self.ds,
            self.db_conn,
            if_exists = 'append',
            index = 'False'
        )