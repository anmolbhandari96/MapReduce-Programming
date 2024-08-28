# Average tips to revenue ratio of the drivers for different pickup locations in sorted format.

from mrjob.job import MRJob

class AverageTipsToRevenueRatioSorted(MRJob):

    def mapper(self, _, line):
        # Skip the header line
        if not line.startswith('VendorID'):
            fields = line.split(',')
            pickup_loc = fields[7]
            total_revenue = float(fields[16])
            tips = float(fields[13])
            yield pickup_loc, (tips, total_revenue)

    def combiner(self, pickup_loc, tips_revenues):
        total_tips = 0
        total_revenue = 0
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        yield pickup_loc, (total_tips, total_revenue)

    def reducer(self, pickup_loc, tips_revenues):
        total_tips = 0
        total_revenue = 0
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        average_tips_to_revenue_ratio = total_tips / total_revenue
        yield pickup_loc, average_tips_to_revenue_ratio


if __name__ == '__main__':
    AverageTipsToRevenueRatioSorted.run()