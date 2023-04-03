class DBWriter():
    def __init__(self, df, db_conn, ds, idx):
        self.df = df
        self.db_conn = db_conn
        self.ds = ds
        self.idx = idx

    def write_to_postgres(self):
        print(f'Writing to {self.ds} table.....')
        try:
            self.df.to_sql(
                self.ds,
                self.db_conn,
                if_exists='append',
                index=False,
                schema = 'retail'
            )
            print(f'\nData chunk {self.idx} with size {self.df.shape[0]} loaded successfully to table retail.{self.ds}')
        except Exception as e:
            print(f'Error occurred while writing data: {e}')