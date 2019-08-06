
from selenium import webdriver
from bs4 import BeautifulSoup # For HTML parsing
import numpy as np
import warnings
import pickle
import re
from kafka import KafkaProducer
from json import dumps
from time import sleep

warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)

def init_driver():
    ''' Initialize chrome driver'''

    chrome_options = webdriver.ChromeOptions()

    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-infobars')
    chrome_options.add_argument('--profile-directory=Default')
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--disable-plugins-discovery")
    chrome_options.add_argument('--disable-popup-blocking')
    chrome_options.add_argument("--start-maximized")
    browser = webdriver.Chrome(chrome_options=chrome_options,executable_path='/Users/liuting/Downloads/chromedriver')



    return browser

def get_pause():
    return np.random.choice(range(4,6))

def get_soup(url):
    """
    Given the url of a page, this function returns the soup object.

    Parameters:
        url: the link to get soup object for

    Returns:
        soup: soup object
    """
    driver =init_driver()
    driver.get(url)
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    driver.close()
    return soup

def grab_job_links(soup):
    """
    Grab all non-sponsored job posting links from a Indeed search result page using the given soup object

    Parameters:
        soup: the soup object corresponding to a search result page
            e.g. https://ww.indeed.com/jobs?q=data+scientist&l=NewYork&start=20

    eturns:
        urls: a python list of job posting urls

    """
    urls = []
    for link in soup.find_all('div', {'class': 'title'}):
        partial_url = link.a.get('href')
        url = 'https://www.indeed.com' + partial_url
        urls.append(url)
    return urls


def get_urls(query, num_pages, location,type='fulltime', experience='entry_level'):
    """
    Get all the job posting URLs resulted from a specific search.

    Parameters:
        query: job title to query
        num_pages: number of pages needed
        location: city to search in

    Returns:
        urls: a list of job posting URL's (when num_pages valid)
        max_pages: maximum number of pages allowed ((when num_pages invalid))
    """
    # We always need the first page
    base_url = 'https://www.indeed.com/jobs?q={}&l={}&jt={}&explvl={}'.format(query, location, type, experience)
    soup = get_soup(base_url)
    urls = grab_job_links(soup)

    # Get the total number of postings found
    posting_count_string = soup.find(name='div', attrs={'id': "searchCount"}).get_text()
    posting_count_string = posting_count_string[posting_count_string.find('of') + 2:-4].replace(',','')
    # print('posting_count_string: {}'.format(posting_count_string))
    # print('type is: {}'.format(type(posting_count_string)))

    try:
        posting_count = int(posting_count_string)
    except ValueError:  # deal with special case when parsed string is "360 jobs"
        posting_count = int(re.search('\d+', posting_count_string).group(0))
        print('posting_count: {}'.format(posting_count))
        print('\ntype: {}'.format(type(posting_count)))
    finally:
        posting_count = 330  # setting to 330 when unable to get the total
        pass

    # Limit nunmber of pages to get
    max_pages = round(posting_count / 10) - 3
    if num_pages > max_pages:
        print('returning max_pages!!')
        return max_pages

        # Additional work is needed when more than 1 page is requested
    if num_pages >= 2:
        # Start loop from page 2 since page 1 has been dealt with above
        for i in range(2, num_pages + 1):
            num = (i - 1) * 10
            base_url = 'https://www.indeed.com/jobs?q={}&l={}&start={}'.format(query, location, num)
            try:
                soup = get_soup(base_url)
                # We always combine the results back to the list
                urls += grab_job_links(soup)
            except:
                continue

    # Check to ensure the number of urls gotten is correct
    # assert len(urls) == num_pages * 10, "There are missing job links, check code!"

    return urls


def get_posting(url):
    """
    Get the text portion including both title and job description of the job posting from a given url

    Parameters:
        url: The job posting link

    Returns:
        title: the job title (if "data scientist" is in the title)
        posting: the job posting content
    """
    # Get the url content as BS object
    soup = get_soup(url)

    # The job title is held in the h3 tag
    title = soup.find(name='h3').getText().lower()
    posting = soup.find(name='div', attrs={'class': "jobsearch-JobComponent"}).get_text()

    return title, posting.lower()

    # if 'data scientist' in title:  # We'll proceed to grab the job posting text if the title is correct
    # All the text info is contained in the div element with the below class, extract the text.
    # posting = soup.find(name='div', attrs={'class': "jobsearch-JobComponent"}).get_text()
    # return title, posting.lower()
    # else:
    # return False

    # Get rid of numbers and symbols other than given
    # text = re.sub("[^a-zA-Z'+#&]", " ", text)
    # Convert to lower case and split to list and then set
    # text = text.lower().strip()

    # return text


def get_data(query, num_pages, location):
    """
    Get all the job posting data and save in a json file using below structure:

    {<count>: {'title': ..., 'posting':..., 'url':...}...}

    The json file name has this format: ""<query>.json"

    Parameters:
        query: Indeed query keyword such as 'Data Scientist'
        num_pages: Number of search results needed
        location: location to search for

    Returns:
        postings_dict: Python dict including all posting data

    """
    # Convert the queried title to Indeed format
    query = '+'.join(query.lower().split())

    postings_dict = {}
    urls = get_urls(query, num_pages, location)

    #  Continue only if the requested number of pages is valid (when invalid, a number is returned instead of list)
    if isinstance(urls, list):
        num_urls = len(urls)
        for i, url in enumerate(urls):
            try:
                title, posting = get_posting(url)
                postings_dict[i] = {}
                postings_dict[i]['title'],postings_dict[i]['category'], postings_dict[i]['city'], \
                postings_dict[i]['posting'], postings_dict[i]['url'] = title,query,location, posting, url
                # for specific topics,here we have three dif topics
                if query=='Data Scientist':
                    topic='IndeedJobs_ds'
                elif query=='Data Engineer':
                    topic='IndeedJobs_de'
                else:
                    topic='IndeedJobs_da'
                # producer.send(topic, value=postings_dict[i])
                producer.send('IndeedJobs',value=postings_dict[i])
                print(postings_dict[i])
                sleep(5)
            except:
                continue

            percent = (i + 1) / num_urls
            # Print the progress the "end" arg keeps the message in the same line
            print("Progress: {:2.0f}%".format(100 * percent), end='\r')

        # Save the dict as pickle file
        save_obj(postings_dict, 'IndeedDict')


        print('All {} postings have been scraped and saved!'.format(num_urls))
        # return postings_dict
    else:
        print("Due to similar results, maximum number of pages is only {}. Please try again!".format(urls))
###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################

jobName_lst = ['Data Scientist', 'Data Analyst','Data Engineer']

city_lst = ['San Jose', 'New York', 'San Francisco',  \
            'Austin', 'Boston', 'Seattle', 'Chicago','Los Angeles']
producer = KafkaProducer(bootstrap_servers=["localhost:9092"],value_serializer=lambda x: dumps(x).encode('utf-8'))
for jobName in jobName_lst:
    for city in city_lst:
        get_data(jobName,20,city)









