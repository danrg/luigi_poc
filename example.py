import luigi


class Aggregate(luigi.Task):
    customer = luigi.Parameter()
    location = luigi.Parameter()

    def requires(self):
        return [Customers(self.customer),
                Locations(self.location)]

    def run(self):
        with self.output().open('w') as file:
            file.write('{"status": "Done"}')

    def output(self):
        return luigi.LocalTarget('aggregated_{}_{}.json'.
                                 format(self.customer,
                                        self.location))


class Customers(luigi.Task):
    first_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('customer_{}.json'.
                                 format(self.first_name))

    def run(self):
        with self.output().open('w') as file:
            file.write('{"status": "Done"}')


class Locations(luigi.Task):
    home_location = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('location_{}.json'.
                                 format(self.home_location))

    def run(self):
        with self.output().open('w') as file:
            file.write('{"status": "Done"}')


if __name__ == '__main__':
    luigi.build([Aggregate(customer='John',
                           location='US')],
                local_scheduler=True)
