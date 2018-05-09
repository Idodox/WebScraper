import bs4 as bs
import requests
import pymysql
import pandas as pd
import multiprocessing as mp
from datetime import datetime
from sqlalchemy import create_engine


# TODO: add user-definable options for scope of download
# TODO: implement threading or multiprocessing


BRANDS_PAGE_LINK = 'https://www.peterglenn.com/brands'
NUM_PROCESSES = 30


def main():
    """ Main Function """


    # Multiprocessing with queue
    # Initiate multiprocessing object
    # pool = multiprocessing.Pool()
    # output = mp.Queue()
    # processes = []
    # for i in range(NUM_PROCESSES):
    #     processes.append(mp.Process(target = get_item_links_for_brand, args=(brands_link_string_list[i], True, output)))
    #     sleep(0.5)
    #     print('process number: ',i, ' starting')
    #     processes[i].start()
    #
    #
    # for p in processes:
    #     print('process number: ',p, ' joining')
    #     p.join()
    #
    # results = [len(output.get()) for p in processes]
    # num_items = sum(results)
    # print(results,num_items)

    new_items_inventory_df = run_site_crawl(BRANDS_PAGE_LINK)

    save_data_to_database(new_items_inventory_df)



""" ****************************************************************** """
""" *************** DATABASE MANAGEMENT FUNCTIONS ******************** """
""" ****************************************************************** """


def get_item_ids(items):
    """********** run mysql command **********"""
    query = """ SELECT item_id, title FROM items WHERE title in ('{}'); """.format(items)

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
        if db_name == 'items':
            df = df[['category', 'brand', 'title', 'msrp']]
            conn = create_engine('mysql+pymysql://root:idofarhi@localhost:3306/scraping', echo=False)
            df.to_sql(name='items', con=conn, index=False, if_exists='append')

        elif db_name == 'inventory':
            df = df[['date_time', 'current_price', 'sizes', 'colors', 'item_id']]
            conn = create_engine('mysql+pymysql://root:idofarhi@localhost:3306/scraping', echo=False)
            df.to_sql(name='inventory', con=conn, index=False, if_exists='append')

        else:
            print('Can only write to "items" or "inventory" tables, you asked to write to: '+db_name)

    except:
        print("Error: unable to run mysql command")


def get_sql_table(table_name):
    """********** run mysql command **********"""

    if table_name != ('items' or 'inventory'):
        print('Can only fetch tables "items" and "inventory", you asked for {}'.format(table_name))
        return []

    try:
        conn = create_engine('mysql+pymysql://root:idofarhi@localhost:3306/scraping', echo=False)
        query_result = pd.read_sql("SELECT * FROM {};".format(table_name), conn)

    except:
        # TODO: Output actual error message if an exception occurs
        # TODO: Catch case of empty table
        print("Error: unable to get sql table")
        return []

    return query_result


def save_data_to_database(new_items_inventory_df):
    """ Runs the process of saving all the new data to the database tables """

    # Getting the current items table to compare with the new items:
    items_df = get_sql_table('items')
    # Find differences between df of new items and items pulled from items table:
    df_to_items_db = items_df.merge(new_items_inventory_df, on=['category', 'brand', 'title', 'msrp'],
                                    how='outer', indicator=True).query("_merge == 'right_only'").drop('_merge', 1)
    # Write new items (if any) to items database:
    write_to_database(df_to_items_db, 'items')

    # Get item id's for collected items (need for reference in inventory db):
    item_ids = get_item_ids(
        "','".join(list(pd.Series(new_items_inventory_df['title']).apply(lambda x: x.replace("'", "''")))))
    # Combime pulled id's with collected items:
    df_to_inventory_db = new_items_inventory_df.merge(item_ids, on='title', how='outer')
    write_to_database(df_to_inventory_db, 'inventory')

    return


""" ****************************************************************** """
""" ************* END DATABASE MANAGEMENT FUNCTIONS ****************** """
""" ****************************************************************** """


def run_site_crawl(brands_page_link):
    """ Runs the step by step main process for crawling the peter glenn website """
    # Step 1: get all the links for different brands from brands page
    brands_link_string_list = get_brands_page_links(brands_page_link)

    # Step 2: iterate through all brands and get links for full item list:
    items_details_list = []
    for brand in brands_link_string_list[:10]:

        item_links = get_item_links_for_brand(brand)
        # Step 3: get the details for all items in the brand's link list:
        brand_items_details_list = get_details_from_brand_links_list(item_links, brand[1])
        items_details_list += brand_items_details_list


    # Putting all the collected brands' items in a dataframe:
    new_items_inventory_df = pd.DataFrame(items_details_list, columns=['date_time', 'category', 'brand', 'title', 'msrp',
                                                                       'current_price', 'sizes', 'colors'])
    return new_items_inventory_df


def get_item_info(link):
    """ Returns item category, title, msrp, current price, sizes and colors"""
    soup, left_soup, right_soup = divide_page_sections(link)
    category = get_category(soup)
    title, msrp, current_price = get_title_msrp_price(left_soup, right_soup)
    print(category, title)
    sizes = get_sizes(right_soup)
    colors = get_colors(right_soup)
    print(msrp, current_price)
    print(sizes)
    print(colors)

    return category, title, msrp, current_price, sizes, colors


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
    """ Get item title, MSRP and price """
    # we split the price at a space and take the first item so that we get
    # the lowest price in case the item has a range of prices
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
        for item in size_soup:
            sizes.append(item.find(class_='size_text').text)
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
        for item in color_soup:
            colors.append(item.get('id'))
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


def get_item_links_for_brand(brand_tuple, multiprocessing = False, queue = None):
    """ Receives a brand, returns a list of links of all items by that brand"""

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
    """ Gets the details for all items in the list of links. """
    lst = []
    for link in list_of_links:
        category, title, msrp, current_price, sizes, colors = get_item_info(link)
        lst.append([datetime.now().strftime("%y-%m-%d-%H-%M"), category, brand, title, msrp, current_price, sizes, colors])

    return lst



if __name__ == '__main__':
    main()
