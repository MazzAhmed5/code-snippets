from django.db import connection
from apps.catalogue.models import Product
from apps.partner.utils import ids_to_str


BATCH_SIZE = 50000

INSERT_PRODUCT = '''
INSERT INTO cheetay.catalogue_productstats (product_id, order_rate, normalized_order_rate, favourite_rate,
 normalized_favourite_rate, novelty_rate, normalized_novelty_rate)
( SELECT 
    id as product_id, 0.0, 0.0, 0.0 , 0.0, 60.0, 0.0
     from catalogue_product
     ) 
'''

UPDATE_ORDER_RATE = '''
INSERT INTO cheetay.catalogue_productstats (product_id, order_rate)
( SELECT 
    product_id , SUM(quantity) order_rate
     from 
     order_line INNER JOIN catalogue_product cp on order_line.product_id = cp.id 
     group by product_id
     ) ON DUPLICATE KEY UPDATE order_rate = VALUES(order_rate)
'''

UPDATE_FAVOURITE_RATE = '''
INSERT INTO cheetay.catalogue_productstats (product_id, favourite_rate)
( SELECT 
    product_id, COUNT(product_id) favourite_rate
     from 
     customer_userproductfavourite group by product_id
     ) ON DUPLICATE KEY UPDATE favourite_rate = VALUES(favourite_rate)
'''

UPDATE_NOVELTY_RATE = '''
INSERT INTO cheetay.catalogue_productstats (product_id, novelty_rate)
( SELECT  P.id as product_id, datediff(CURDATE(), P.date_created) as novelty_rate
 from
  (select date_created, id
  from
   catalogue_product where date_created >= now()-interval 2 month)
    as P
    ) ON DUPLICATE KEY UPDATE novelty_rate = VALUES(novelty_rate) 
'''

PRODUCT_STATS_MIN_MAX = '''
SELECT 
    IFNULL(MIN(order_rate), 0), IFNULL(MAX(order_rate), 1),
    IFNULL(MIN(favourite_rate), 0), IFNULL(MAX(favourite_rate), 1),
    IFNULL(MIN(novelty_rate), 0), IFNULL(MAX(novelty_rate), 1)
FROM
    cheetay.catalogue_productstats
'''

PRODUCT_NORMALIZE_STATS = '''
UPDATE cheetay.catalogue_productstats 
SET 
    normalized_order_rate = (order_rate - {min_order_rate}) / {order_rate_divisor},
    normalized_favourite_rate = (favourite_rate - {min_favourite_rate}) / {favourite_rate_divisor},
    normalized_novelty_rate = ((60 - novelty_rate) - (60 - {max_novelty_rate})) / {novelty_rate_divisor}
'''

UPDATE_PRODUCT_SCORE = '''
UPDATE cheetay.catalogue_product p,
    cheetay.catalogue_productstats as ps
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
        cursor.execute('TRUNCATE TABLE cheetay.catalogue_productstats')

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
