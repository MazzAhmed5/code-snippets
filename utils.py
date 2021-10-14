import operator
from collections import defaultdict

from apps.utils import get_absolute_image_uri
from apps.catalogue.models import Category
from apps.cache_manager import CacheManager
from django.core.cache import cache
from django.utils import timezone as django_timezone
from apps.utils import extract_day_and_time
from django.db.models import Q
from django.conf import settings
from apps.catalogue.models import Product
from apps.search.deals_search_handler import DealSearchHandler


def get_category_dict(category, parent=True):
    """
    Helper to form category dict

    Parameters
    ----------
    category: Category
    parent: bool
        adds image attribute if parent

    Returns
    -------
    category_dict: dict
        {'id': '', 'name', '' ...}

    """
    category_dict = {
        'id': category.id,
        'name': category.name,
        'priority': category.priority,
    }
    if parent:
        category_dict['image'] = category.mobile_image.url if category.mobile_image else None

    return category_dict

def get_sorted_categories(category_dict):
    """
        Helper to form sorted dict based on name and priority

    Parameters
    ----------
    category_dict: Category dict

    Returns
    -------
    category_dict: dict
        {'id': '', 'name', '' ...}


    """

    categories_values = list(category_dict)
    categories_values.sort(key=operator.itemgetter('name'))
    categories_values.sort(key=operator.itemgetter('priority'), reverse=True)
    for index, value in enumerate(categories_values):
        if value['child']:
            categories_values[index]['child'].sort(key=operator.itemgetter('name'))
            categories_values[index]['child'].sort(key=operator.itemgetter('priority'), reverse=True)

    return categories_values


def get_category_hierarchy(category_ids):
    """
    Prepare category hierarchy response for depth > 1. Flattens all the categories with depth >2 under common parent.

    Parameters
    ----------
    category_ids: set
        List of category id's either child or parent

    Returns
    -------
    object: list of dict
        in following format [ { id: '', image:'' , name:'' , child : [{id:'' , name:'' }]} ] or []
    """
    if not category_ids:
        return []

    min_depth = 2
    categories = Category.objects.filter(id__in=category_ids, depth__gte=min_depth).only(
        'name', 'mobile_image', 'path', 'depth', 'priority'
    ).distinct()

    parent_categories_map = defaultdict(lambda: {'child': []})
    child_categories_map = {}
    l3_parent_category_paths = set()

    for category in categories:
        if category.depth == min_depth:
            parent_categories_map[category.path].update(get_category_dict(category))
        elif category.depth == min_depth + 1:
            child_categories_map[category.path] = category
        else:
            l3_parent_category_paths.add(category.path[0:Category.steplen * (min_depth + 1)])

    l3_parent_category_paths = {path for path in l3_parent_category_paths if path not in child_categories_map.keys()}
    child_categories_map.update(
        {c.path: c for c in Category.objects.filter(path__in=l3_parent_category_paths).only('name', 'path')}
    )

    missing_category_paths = set()
    for category in child_categories_map.values():
        parent_path = category.path[0:Category.steplen * min_depth]
        if parent_path not in parent_categories_map.keys():
            missing_category_paths.add(parent_path)
        parent_categories_map[parent_path]['child'].append(get_category_dict(category, parent=False))

    missing_parent_categories = Category.objects.filter(path__in=missing_category_paths).only(
        'name', 'mobile_image', 'path', 'priority'
    )

    for category in missing_parent_categories:
        parent_categories_map[category.path].update(get_category_dict(category))

    return get_sorted_categories(parent_categories_map.values())


def error_json_response(message):
    return {'message': message}


def get_feature_dict(name, slug):
    return {'id': None, 'name': name, 'slug': slug}

def active_deal_type_partners(partner_category, filter_deal_type, user_location, area, city_id):
    """
    Find partners with applicable deals using given params
    Parameters
    ----------
    partner_category: str
        deal partners of given category will be extracted.
    filter_deal_type: list
        List of deal type filter (used for web)
    user_location: Point
        user location to extract location based deal partners
    area: apps.partner.models.DeliveryArea
        area of user to extract offers in that area.
    city_id: int
        city for which offers will be extracted

    Returns
    -------
    set
        set of partner ids which do have a deal available for user city, area and location.

    """
    deal_type_partner_stocks = get_deal_type_product_stocks(partner_category, filter_deal_type, user_location, area, city_id)
    deal_type_partners = set(stock[1] for stock in deal_type_partner_stocks)
    return deal_type_partners

def get_deal_type_product_stocks(partner_category, filter_deal_type, user_location, area, city_id):
    """
    Get products which do have a deal. (used for web)
    Parameters
    ----------
    partner_category: str
        category for which products will be extracted
    filter_deal_type: list
        list of filters to find specific deals
    user_location: Point
        location of user
    area: apps.partner.models.DeliveryArea
        area of user
    city_id: int
        city id for which deals need to be extracted

    Returns
    -------
    list
        list of tuples containing tuples of product and partner id

    """
    deal_type_stocks = CacheManager.get_all_deal_type_stocks(partner_category, city_id)
    excluded_products = DealSearchHandler.get_excluded_products(user_location, area)

    deal_type_stocks = list(filter(lambda stock: stock[0] not in excluded_products, deal_type_stocks))

    if filter_deal_type:
        deal_type_products = Product.objects.filter(deal_type__slug__in=filter_deal_type).values_list('id', flat=True)
        deal_type_stocks = list(filter(lambda stock: stock[0] in deal_type_products, deal_type_stocks))

    return deal_type_stocks