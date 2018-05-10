import bs4 as bs
import requests
import pymysql
import pandas as pd
import concurrent.futures
from datetime import datetime
from sqlalchemy import create_engine


# TODO: add user-definable options for scope of download
# TODO: make soup searches more efficient (so don't need soup left & right)
# TODO: if title starts with brand name, remove it from title.


BRANDS_PAGE_LINK = 'https://www.peterglenn.com/brands'
NUM_CONCURRENT_PROCESSES = 20
STARTING_BRAND_NUM = 112 # Use ":" to start at first brand
NUM_BRANDS_TO_SCRAP = 113 # Use ":" to scrap all brands


def main():
    """
    Main Function
    1. Runs site crawl
    2. Saves crawled data to the database
    """

    new_products_inventory_df = run_site_crawl(BRANDS_PAGE_LINK)

    save_data_to_database(new_products_inventory_df)

    return


""" ****************************************************************** """
""" *************** DATABASE MANAGEMENT FUNCTIONS ******************** """
""" ****************************************************************** """


def get_product_ids(products):
    """********** run mysql command **********"""
    query = """ SELECT product_id, title FROM products WHERE title in ('{}'); """.format(products)

    try:
        conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='idofarhi', db='scraping')
        cur = conn.cursor()
        cur.execute("USE scraping")
        query_result = pd.read_sql(query, conn)
        cur.close()
        conn.close()

    except:
        print("Error: unable to fetch data")
        return

    return query_result


def write_to_database(df,db_name):
    """********** run mysql command **********"""

    try:
        if db_name == 'products':
            df = df[['product_id', 'category', 'brand', 'title', 'msrp']]
            conn = create_engine('mysql+pymysql://root:idofarhi@localhost:3306/scraping', echo=False)
            df.to_sql(name='products', con=conn, index=False, if_exists='append')

        elif db_name == 'inventory':
            df = df[['date_time', 'current_price', 'sizes', 'colors', 'product_id']]
            conn = create_engine('mysql+pymysql://root:idofarhi@localhost:3306/scraping', echo=False)
            df.to_sql(name='inventory', con=conn, index=False, if_exists='append')

        else:
            print('Can only write to "products" or "inventory" tables, you asked to write to: '+db_name)

    except:
        print("Error: unable to run mysql command. ERROR 1")


def get_sql_table(table_name):
    """********** run mysql command **********"""

    if table_name != ('products' or 'inventory'):
        print('Can only fetch tables "products" and "inventory", you asked for {}'.format(table_name))
        return []

    try:
        conn = create_engine('mysql+pymysql://root:idofarhi@localhost:3306/scraping', echo=False)
        query_result = pd.read_sql("SELECT * FROM {};".format(table_name), conn)

    except:
        # TODO: Output actual error message if an exception occurs
        # TODO: Catch case of empty table separately
        print("Error: unable to get sql table. ERROR 2")
        return []

    return query_result


def save_data_to_database(new_products_inventory_df):
    """ Runs the process of saving all the new data to the database tables """

    # Getting the current products table to compare with the new products:
    products_df = get_sql_table('products')
    # Find differences between df of new products and products pulled from products table:
    df_to_products_db = products_df.merge(new_products_inventory_df, on=['product_id', 'category', 'brand', 'title', 'msrp'],
                                    how='outer', indicator=True).query("_merge == 'right_only'").drop('_merge', 1)
    # Write new products (if any) to products database:
    write_to_database(df_to_products_db, 'products')

    # Get product id's for collected products (need for reference in inventory db):
    product_ids = get_product_ids(
        "','".join(list(pd.Series(new_products_inventory_df['title']).apply(lambda x: x.replace("'", "''")))))
    # Combime pulled id's with collected products:
    df_to_inventory_db = new_products_inventory_df.merge(product_ids, on='product_id', how='outer')
    write_to_database(df_to_inventory_db, 'inventory')

    return


""" ****************************************************************** """
""" ************* END DATABASE MANAGEMENT FUNCTIONS ****************** """
""" ****************************************************************** """


def run_site_crawl(brands_page_link):
    """ Runs the step by step main process for crawling the peter glenn website """

    # Step 1: get all the links for different brands from brands page
    brands_link_string_list = get_brands_page_links(brands_page_link)

    products_details_list = []

    def run_through_brands_links(brand):

        # Step 2: iterate through all brands and get a list of links for all their products:
        product_links = get_product_links_for_brand(brand)
        # Step 3: get the details for all products in the brand's link list:
        print('\033[93m' + 'Starting crawl for brand: ' + brand[1]+ '\033[0m')
        brand_products_details_list = get_details_from_brand_links_list(product_links, brand[1])

        nonlocal products_details_list
        print('\033[92m'+ 'Finished collecting for brand: '+brand[1]+'\033[0m')
        products_details_list += brand_products_details_list
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_CONCURRENT_PROCESSES) as executer:
        executer.map(run_through_brands_links, brands_link_string_list[STARTING_BRAND_NUM:NUM_BRANDS_TO_SCRAP])

    # Putting all the collected brands' products in a dataframe:
    new_products_inventory_df = pd.DataFrame(products_details_list, columns=['date_time', 'category', 'brand', 'title', 'msrp',
                                                                       'current_price', 'sizes', 'colors', 'product_id'])
    return new_products_inventory_df


def get_product_info(link):
    """ Returns product_id, category, title, msrp, current price, sizes and colors"""
    soup, left_soup, right_soup = divide_page_sections(link)
    category = get_category(soup)
    title, msrp, current_price = get_title_msrp_price(left_soup, right_soup)
    sizes = get_sizes(right_soup)
    colors = get_colors(right_soup)
    product_id = soup.select_one("span[itemprop*=productID]").text
    # print(sizes, colors)
    print('productID:', product_id, '| category:', category, '| title:', title, '| msrp:', msrp, '| current price:', current_price)

    return category, title, msrp, current_price, sizes, colors, product_id


def divide_page_sections(link):
    """ Divide the page into large sections """
    soup = get_soup(link)
    left_soup = (soup.find_all('div'))[1].contents[1].find('div', id='product_page_left')
    right_soup = (soup.find_all('div'))[1].contents[1].find('div', id='product_page_right')

    return soup, left_soup, right_soup


def get_category(soup):
    """ Get category information """
    category_soup = soup.find('div', class_='breadcrumb').find_all('a')
    category = ''
    for subcategory in category_soup:
        if subcategory.text == 'Home':
            continue
        category += subcategory.text + " "
    category = category[:-1]

    return category.replace("_", " ")


def get_title_msrp_price(left_soup, right_soup):
    # TODO: Currently only takes lowest price. Add ability to pull price by color/size
    """ Get product title, MSRP and price """
    # we split the price at a space and take the first product so that we get
    # the lowest price in case the product has a range of prices
    current_price = float(right_soup.find('span', itemprop='price').text.split(' ', 1)[0][1:])
    title = left_soup.find('h1', class_="title").text
    try:
        # Catch exception in case no msrp found
        msrp = float(right_soup.find('span', itemprop='price_msrp').text[1:])
    except AttributeError:
        # no different price & msrp. They are equal.
        msrp = current_price

    return title, msrp, current_price


def get_sizes(right_soup):
    """ Get the sizes """
    sizes = []
    try:
        size_soup = right_soup.find_all('div', class_='choice')
        for product in size_soup:
            sizes.append(product.find(class_='size_text').text)
        if sizes == []:
            sizes = ['O/S']
    except AttributeError:
        # No size specified.
        sizes = ['N/A']
    return ",".join(sizes)


def get_colors(right_soup):
    """ Get the colors """
    colors = []
    try:
        color_soup = right_soup.find('div', class_='color').find_all('img')
        for product in color_soup:
            colors.append(product.get('id'))
    except AttributeError:
        # No color specified.
        colors = ['N/A']

    return ",".join(colors).replace('_', ' ')


def get_soup(link):
    """ Returns html code from link """
    sauce = requests.get(link)
    sauce.encoding = 'ISO-8859-1'

    soup = bs.BeautifulSoup(sauce.content, 'lxml')

    return soup


def get_brands_page_links(brands_page_link):
    """
    Takes the link for brands page and extracts a link and name string for each brand.
    Returns a list with pairs of a brand link and brand string
    """
    soup = get_soup(brands_page_link)
    all_brands_boxes = soup.find('div', id='content-content')
    all_brands_links = all_brands_boxes.find_all('a')

    # Get brand links
    brand_links = [link.get('href') for link in all_brands_boxes.select('div.brand_name > a:nth-of-type(1)')]
    # add https://www.peterglenn.com to the beginning of each link
    site = "https://www.peterglenn.com"
    brand_links = [site + link for link in brand_links]
    # Get brand name strings
    brand_strings = [str(brand.string) for brand in all_brands_links]
    # Combine to a dictionary
    brands_link_string_list = list(zip(brand_links, brand_strings))
    # print(brands_link_string_dict)

    return brands_link_string_list


def get_product_links_for_brand(brand_tuple, multiprocessing = False, queue = None):
    """ Receives a brand, returns a list of links of all products by that brand"""

    brand_page_link = brand_tuple[0]

    soup = get_soup(brand_page_link)

    try:
        # Find last page. Page n actually refers to n+1 on the site
        last_page = int(soup.find('a', string='Last')['href'][-1])
    except TypeError:
        # If this happens it means the brand has only one page, so the pages
        # constructor doesn't exist and can't be found. This process is OK
        last_page = 0

    links = []
    for page in range(last_page + 1):
        soup = get_soup(brand_page_link + '?page=%d' % page)
        link_soup = soup.find_all('div', class_='category_result_image')
        links += [link.a.get('href') for link in link_soup]
        # Avoid overloading site:
        # sleep(0.5)

    if multiprocessing:
        print(len(links))
        queue.put(links)

    return links


def get_details_from_brand_links_list(list_of_links, brand):
    """ Gets the details for all products in the list of links. """
    lst = []
    for link in list_of_links:
        category, title, msrp, current_price, sizes, colors, product_id = get_product_info(link)
        lst.append([datetime.now().strftime("%y-%m-%d-%H-%M"), category, brand, title, msrp, current_price, sizes, colors, product_id])

    return lst



if __name__ == '__main__':
    main()
