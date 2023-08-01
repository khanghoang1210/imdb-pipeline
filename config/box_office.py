
class BoxOffice:
    def __init__(self, id=None, rank=None, revenue=None, partition_date=None):
        self.id = id
        self.rank = rank
        self.revenue = revenue
        self.partition_date = partition_date

    # def __init__(self):
    #     pass

    # def show(self):
    #   print(f"Movie info: [{self.id}]-[{self.revenue}]")

    def __str__(self):
        return f"BoxOffice(id={self.id}, rank={self.rank}, revenue={self.revenue}, partition_date={self.partition_date})"
    def get_name(self):
        return self.id, self.rank, self.revenue, self.partition_date
    def to_dict(self):
        return {
            'rank': self.rank,
            'revenue': self.revenue,
            'partition_date': self.partition_date,
            'id': self.id,
        }

