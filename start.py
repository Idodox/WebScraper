import bs4 as bs
import requests
import pandas as pd
import concurrent.futures
from datetime import datetime
from sqlalchemy import create_engine
from sys import exc_info
import logging

startTime = datetime.now()

# TODO: add user-definable options for scope of download
# TODO: if title starts with brand name, remove it from title.
# TODO: Sort companies by number items when scraping (hopefully shorter total time to scrape)
# TODO: add categories table and brands table
# TODO: Update products MSRP/TITLE/etc with new changes (provided same link)
# TODO: Improve logging and usage of logging library


# Constants
BRANDS_PAGE_LINK = 'https://www.peterglenn.com/brands'
NUM_CONCURRENT_PROCESSES = 40
STARTING_BRAND_NUM = 0 # Use 0 to do a full run
# ENDING_BRAND_NUM = 50


def main():
    """
    Main Function
    1. Runs site crawl
    2. Saves crawled data to the database
    """

    new_products_inventory_df = run_site_crawl(BRANDS_PAGE_LINK)

    save_data_to_database(new_products_inventory_df)

    print('Total run time:', datetime.now() - startTime)

    return


class WebsiteDownException(Exception):
    pass

""" ****************************************************************** """
""" *************** DATABASE MANAGEMENT FUNCTIONS ******************** """
""" ****************************************************************** """


def get_product_ids(products):
    """********** run mysql command **********"""
    query = """ SELECT product_id, link FROM products WHERE link in ('{}'); """.format(products)

    try:
        conn = create_engine('mysql+pymysql://root:idofarhi@localhost:3306/scraping?charset=utf8', echo=False, encoding='UTF-8')

        query_result = pd.read_sql(query, conn)

    except:
        print("Error: unable to fetch product_id, title", exc_info())
        return

    return query_result


def write_to_database(df,db_name):
    """********** run mysql command **********"""

    try:
        if db_name == 'products':
            df = df[['category', 'brand', 'title', 'msrp', 'link']]
            conn = create_engine('mysql+pymysql://root:idofarhi@localhost:3306/scraping?charset=utf8', echo=False, encoding='UTF-8')
            df.to_sql(name='products', con=conn, index=False, if_exists='append')

        elif db_name == 'inventory':
            df = df[['product_id', 'date_time', 'current_price', 'sizes', 'colors']]
            conn = create_engine('mysql+pymysql://root:idofarhi@localhost:3306/scraping?charset=utf8', echo=False, encoding='UTF-8')
            df.to_sql(name='inventory', con=conn, index=False, if_exists='append')

        else:
            print('Can only write to "products" or "inventory" tables, you asked to write to: '+db_name)

    except:
        print("Error: unable to write to database. ERROR 1",  exc_info())


def get_sql_table(table_name):
    """********** run mysql command **********"""

    if table_name != ('products' or 'inventory'):
        print('Can only fetch tables "products" and "inventory", you asked for {}'.format(table_name))
        return []

    try:
        conn = create_engine('mysql+pymysql://root:idofarhi@localhost:3306/scraping?charset=utf8', echo=False, encoding='UTF-8')
        query_result = pd.read_sql("SELECT * FROM {};".format(table_name), conn)

    except:
        # TODO: Catch case of empty table separately
        print("Error: unable to get sql table. ERROR 2",  exc_info())
        return []

    return query_result



""" ****************************************************************** """
""" ************* END DATABASE MANAGEMENT FUNCTIONS ****************** """
""" ****************************************************************** """




def save_data_to_database(new_products_inventory_df):
    """ Runs the process of adding the scraped data to the database tables """

    # Getting the current products table to compare with the new products:
    print('\033[92m' + 'Getting the current products table to compare with the new products:' + '\033[0m')
    products_df = get_sql_table('products')

    # Find differences between df of new products and products pulled from products table:
    print('\033[92m' + 'Finding any new products...' + '\033[0m')
    df_to_products_db = products_df.merge(new_products_inventory_df, on='link', how='outer', indicator=True)\
        .query("_merge == 'right_only'").drop(['category_x', 'brand_x', 'title_x', 'msrp_x', '_merge'], 1)
    df_to_products_db.columns = df_to_products_db.columns.str.replace('_y', '')

    if len(df_to_products_db) == 0:
        print('\033[92m' + 'No new products found' + '\033[0m')
    else:
        print('\033[92m' + str(len(df_to_products_db)) + ' new products found. Writing them to products db...' + '\033[0m')
        # Write new products to products database:
        write_to_database(df_to_products_db, 'products')

    # Get product id's for collected products (need for reference in inventory db):
    print('\033[92m' + "Getting product ID's for collected products" + '\033[0m')
    product_ids = get_product_ids("','".join(list(pd.Series(new_products_inventory_df['link']))))

    # Combime the product_id's from database (based on unique link) to collected products:
    df_to_inventory_db = new_products_inventory_df.merge(product_ids, on='link', how='outer')
    print('\033[92m' + "Writing scraped data to db..." + '\033[0m')
    write_to_database(df_to_inventory_db, 'inventory')

    print('\033[92m' + "Scrape finished successfully at: " + datetime.now().strftime("%y-%m-%d-%H-%M") + '\033[0m')

    return


def run_site_crawl(brands_page_link):
    """ Runs the step by step main process for crawling the peter glenn website """

    # Step 1: get all the links for different brands from brands page
    brands_link_string_list = get_brands_page_links(brands_page_link)
    products_details_list = []
    num_items = 0

    def run_through_brands_links(brand):

        # Step 2: iterate through all brands and get a list of links for all their products:
        product_links = get_product_links_for_brand(brand)

        # Count how many items to scrape
        nonlocal num_items
        num_items += len(product_links)
        print('sum items so far: '+str(num_items), '| Number of items for brand ' + brand[1], ':' + str(len(product_links)))

        # Step 3: get the details for all products in the brand's link list:
        print('\033[93m' + 'Starting crawl for brand: ' + brand[1]+ '\033[0m')
        brand_products_details_list = get_details_from_brand_links_list(product_links, brand[1])

        nonlocal products_details_list
        print('\033[92m' + 'Finished collecting for brand: ' + brand[1] + '\033[0m')
        products_details_list += brand_products_details_list
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_CONCURRENT_PROCESSES) as executer:
        executer.map(run_through_brands_links, brands_link_string_list[27:28], timeout= 500)
        # for fut in concurrent.futures.as_completed(fs):
        #     fut.result()

    print('\033[91m' + '********** Finished ALL collections **********' + '\033[0m')

    # Putting all the collected brands' products in a dataframe:
    new_products_inventory_df = pd.DataFrame(products_details_list, columns=['date_time', 'category', 'brand', 'title', 'msrp',
                                                                       'current_price', 'sizes', 'colors', 'link'])
    return new_products_inventory_df


def get_product_info(link):
    """ Returns product_id, category, title, msrp, current price, sizes and colors"""
    soup = get_soup(link)
    category = get_category(soup)
    title = soup.find('h1', class_="title").text

    # Only want to run scrape if OoS box doesn't exist
    if soup.find('div', class_ = "oos_box_top") is None:
        msrp, current_price = get_title_msrp_price(soup)
        sizes = get_sizes(soup)
        colors = get_colors(soup)
    else: # If this runs item is out of stock
        msrp, current_price, sizes, colors = None, None, None, None

    # print(sizes, colors)

    return category, title, msrp, current_price, sizes, colors


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


def get_product_links_for_brand(brand_tuple):
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
        # get links from brand page number *page* and add them to links list
        links += [link.a.get('href') for link in link_soup]

    return links


def get_details_from_brand_links_list(list_of_links, brand):
    """ Gets the details for all products in the list of links. """
    lst = []
    for i, link in enumerate(list_of_links):

        category, title, msrp, current_price, sizes, colors = get_product_info(link)
        print(brand, str(i + 1) + '/' + str(len(list_of_links)), title)
        # print(str(i+1) + '/' + str(len(list_of_links))+ ' |ctgry:', category, '|ttl:', title, '|msrp:', msrp, '|price:', current_price, '|sizes:', sizes, '|colors:', colors)

        lst.append([datetime.now().strftime("%y-%m-%d-%H-%M"), category, brand, title, msrp, current_price, sizes, colors, link[35:]])

    return lst


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


def get_title_msrp_price(soup):
    # TODO: Currently only takes lowest price. Add ability to pull price by color/size
    """ Get product title, MSRP and price """
    # we split the price at a space and take the first product so that we get
    # the lowest price in case the product has a range of prices
    current_price = float(soup.find('span', itemprop='price').text.split(' ', 1)[0][1:])
    try:
        # Catch exception in case no msrp found
        msrp = float(soup.find('span', itemprop='price_msrp').text[1:])
    except AttributeError:
        # no different price & msrp. They are equal.
        msrp = current_price

    return msrp, current_price


def get_sizes(soup):
    """ Get the sizes """
    sizes = []
    try:
        size_soup = soup.find_all('div', class_='choice')
        for product in size_soup:
            sizes.append(product.find(class_='size_text').text)
        if sizes == []:
            sizes = ['O/S']
    except AttributeError:
        # No size specified.
        sizes = ['N/A']
    return ",".join(sizes)


def get_colors(soup):
    """ Get the colors """
    colors = []
    try:
        color_soup = soup.find('div', class_='color').find_all('img')
        for product in color_soup:
            colors.append(product.get('id'))
    except AttributeError:
        # No color specified.
        colors = ['N/A']

    return ",".join(colors).replace('_', ' ')


def get_soup(link):
    """ Returns html code from link """
    try:
        sauce = requests.get(link, timeout = 10)
        sauce.encoding = 'ISO-8859-1'

        if sauce.status_code >= 400:
            logging.warning("Website %s returned status_code=%s" % (link, sauce.status_code))
            raise WebsiteDownException()
        soup = bs.BeautifulSoup(sauce.content, 'lxml')
    except requests.exceptions.RequestException:
        logging.warning("Timeout expired for website %s" % link)
        raise WebsiteDownException()

    return soup


if __name__ == '__main__':
    main()
