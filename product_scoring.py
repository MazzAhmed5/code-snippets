from django.db import connection
from apps.catalogue.models import Product
from apps.partner.utils import ids_to_str


UPDATE_PRODUCT_SCORE = '''
UPDATE catalogue_product p,
    catalogue_productstats as ps
SET 
     p.score = 0.43 * IFNULL(ps.normalized_order_rate, 0) + 0.43 * IFNULL(ps.normalized_favourite_rate, 0) +
     0.14 * IFNULL(ps.normalized_novelty_rate, 0)
WHERE
    ps.product_id in ({product_ids}) and p.id = ps.product_id
'''


def update_product_score():

    """
    calculating the score of products

    1. calculate sum of products using quantity of products ordered by users.
    2. calculate count of favourite products marked by users.
    3. calculate novelty of products from the date of creation.
    4. calculating min, max of order_sum, favourite_count and novelty.
    5. normalize order_sum, favourite_count and novelty.
    6. calculating score of products using order_sum, favourite_count and novelty.
    """

    with connection.cursor() as cursor:
        cursor.execute('TRUNCATE TABLE catalogue_productstats')

    with connection.cursor() as cursor:
        cursor.execute(INSERT_PRODUCT)
        cursor.execute(UPDATE_ORDER_RATE)
        cursor.execute(UPDATE_FAVOURITE_RATE)
        cursor.execute(UPDATE_NOVELTY_RATE)

        cursor.execute(PRODUCT_STATS_MIN_MAX)

        (min_order_rate, max_order_rate,
         min_favourite_rate, max_favourite_rate,
         min_novelty_rate, max_novelty_rate) = cursor.fetchone()

        cursor.execute(PRODUCT_NORMALIZE_STATS.format(
            min_order_rate=min_order_rate, min_favourite_rate=min_favourite_rate,
            max_novelty_rate=max_novelty_rate,
            order_rate_divisor=(max_order_rate - min_order_rate) or max_order_rate or 1,
            favourite_rate_divisor=(max_favourite_rate - min_favourite_rate) or max_favourite_rate or 1,
            novelty_rate_divisor=((60 - min_novelty_rate) - (60 - max_novelty_rate)) or max_novelty_rate or 1))

        product_ids = Product.objects.filter(published=True).values_list('pk', flat=True)
        total_products = len(product_ids)
        for i in range(0, total_products, BATCH_SIZE):
            cursor.execute(UPDATE_PRODUCT_SCORE.format(
                product_ids=ids_to_str(product_ids[i:min(i + BATCH_SIZE, total_products)])))
