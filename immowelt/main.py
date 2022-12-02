import cloudscraper
import datetime
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


def request_to_html(url, scraper):
    # conn = http.client.HTTPSConnection("api.scrapingant.com")
    # conn.request("GET", "/v1/general?url=" + url, headers=headers)
    # res = conn.getresponse()
    # data = res.read()
    # return BeautifulSoup(json.loads(data.decode("utf-8"))["content"], "html.parser")
    data = scraper.get(url).text
    return BeautifulSoup(data, "html.parser")


def parse_listing_new(listing_html: BeautifulSoup):
    title = listing_html.find("h1").text
    
    hard_facts = listing_html.find("app-hardfacts")
    portal_data = listing_html.find("app-expose").find("app-portal-data")


    area = hard_facts.find_all("span")[0].text
    area = area.split(" ")[0]

    rooms = hard_facts.find_all("span")[1].text
    
    
    price = hard_facts.find("strong").text
    price = price.split(u"\xa0")[0].replace(".","")
    
    comission = listing_html.find("div", {"data-cy": "commission"}).find("p").text.split("%")[0].replace(".", ",")
    
    address = listing_html.find("app-estate-address").text
    address = address.replace("Straße nicht freigegeben", "")
    address = address.split(u"\xa0")[0]


    online_id = "" # portal_data.find_all("p")[0].text
    reference_number = "" # portal_data.find_all("p")[1].text
    
    try:
        offerer_info = listing_html.find("div", class_="offerer__details")
        offerer_address = offerer_info.find("p", {'data-cy': 'offerer-address'}).text
        offerer_name = offerer_info.find("h3").text
        contact_info = listing_html.find("app-commercial-offerer")
    except:
        offerer_address = np.nan
        offerer_name = np.nan
        contact_info = np.nan

    # listing_html.find("div", {"data-cy": "contactperson"})
    
    

    try:
        phone_number = contact_info.find("sd-show-number").find("a", href=True).text
    except:
        phone_number = "not available"



    # in "die Wohnung umbenennen und erweitern."
    # additional_info = [i.text for i in object_info.find_all("li")] # .find("app-estate-object-informations") # in 
    die_wohnung = listing_html.find("app-estate-object-informations").get_text()
    vermietet = "vermietet" in die_wohnung.lower()

    
    if listing_html.find("p", text="Wohnungslage"):
        wohnlage = listing_html.find("p", text="Wohnungslage").nextSibling.text
    else:
        wohnlage = np.nan
    
    
    hausgeld = listing_html.find(class_="price card").find("sd-cell").get_text().replace(u"\xa0", "")
    hausgeld = re.findall(r"Hausgeld (\d{1,}\.?\d{1,})", hausgeld) # hausgeld bei 1000er https://www.immowelt.de/expose/26khm5r
    hausgeld = np.nan if len(hausgeld) == 0 else hausgeld[0].replace(".", "")

    try:
        baujahr = re.findall(r"Baujahr: (\d{4})", die_wohnung)[0]
    except:
        baujahr = np.nan


    # Energieträger hinzufügen
    if listing_html.find("p", text="Effizienzklasse"):
        effizienzklasse = listing_html.find("p", text="Effizienzklasse").nextSibling.text
    else:
        effizienzklasse = np.nan

    if listing_html.find("p", text="Wesentliche Energieträger"):
        energieträger = listing_html.find("p", text="Wesentliche Energieträger").nextSibling.text
    elif listing_html.find("p", text="Energieträger"):
        energieträger = listing_html.find("p", text="Energieträger").nextSibling.text
    else:
        energieträger = np.nan

    try:
        bezug = listing_html.find_all("p", text="Bezug")[0].nextSibling.text
    except:
        bezug = np.nan


    # details = "###".join([tag.text for tag in listing_html.find_all("sd-read-more")])  # in Lage und Details auftrennen
    

    if listing_html.find("h2", text="Lage"):
        # lage = listing_html.find("h2", text="Lage").nextSibling
        lage = listing_html.find("app-location").get_text()
    else:
        lage = np.nan

    if listing_html.find("app-texts"):
        details = listing_html.find("app-texts").get_text()
    else:
        details = np.nan # ka durch NA ersetzen

    return [title, area, rooms, address, vermietet, offerer_address, offerer_name,
           phone_number, comission, die_wohnung, wohnlage, hausgeld, baujahr, energieträger, effizienzklasse, bezug, lage, details]


def get_listings_from_page(page_html):
    """
    Get all of the listings that are on a single search page.
    """


    df_new = pd.DataFrame(columns=["link", "id", "price"])
    listings = page_html.findAll("div", {"class" : lambda L: L and L.startswith('Estate')})
    for idx, listing in enumerate(listings):
        href = listing.find("a")["href"]
        id_ = listing.find("a")["href"].split("/")[-1]
        price = listing.find("div", {"data-test":"price"}).text
        price = price.split(" ")[0].replace(".", "")
        df_new = pd.concat((df_new, pd.DataFrame({"link":[href], "id":[id_], "price":[price]})))
    
    return df_new


def get_current_listings(scraper, url, max_pages=np.inf):#
    """
    Get the current listings from all of the search results
    """

    df = pd.DataFrame(columns=["link", "id", "price"])
    i = 0

    while i < max_pages:
        i +=1
        print(f"Scraping page {i} of the listings")
        html = request_to_html(url + str(i), scraper)
        listings = get_listings_from_page(html)
        if len(listings) == 0:
            print(f"Page {i} did not contain any results, stopping scraping process.")
            break
        df = pd.concat((df, listings))

    return df


def main(DATA_PATH, search_url, safe_mode=False):

    # html = request_to_html(search_url, scraper)

    df_new = get_current_listings(scraper, search_url)
    id_ = max([int(i.split("_")[0]) for i in os.listdir(DATA_PATH)] + [0]) + 1
    print(id_)
    # print(DATA_PATH + "\\"+ id_ + current_time + "results.csv")
    df_new.to_csv(os.path.join(DATA_PATH, f"{id_}_{current_time} results.csv"), index=False)

    old_res_name = [i for i in os.listdir(DATA_PATH) if int(i.split("_")[0]) == id_-1]
    results_old = pd.read_csv(os.path.join(DATA_PATH, old_res_name[0]))
    
    to_scrape = df_new.loc[~df_new["id"].isin(results_old["id"])]

    print(to_scrape)

    if not "scraping_results_0" in os.listdir(DATA_PATH):
        df = pd.DataFrame(columns=["id", "price", "link", "first_price", "title", "area", "rooms", 
                            "address", "vermietet", "offerer_address", 
                            "offerer_name", "phone_number", "commission", "die Wohnung", 
                            "wohnlage", "Hausgeld", "Baujahr", "energieträger", "effizienzklasse", 
                            "bezug", "lage", "details", "scraped_at", "active"])
        # df.to_csv(os.path.join(path_to_results, "scraping_results_0.csv"))
        df.to_csv(os.path.join(DATA_PATH, "0_scraping_results.csv"))

        
    old_df = pd.read_csv(
            os.path.join(DATA_PATH, f"{str(id_-1)}_scraping_results.csv")
            , index_col=0) # open that version

    old_df = df_new[["id", "price"]].merge(old_df, how="right", right_on="id", left_on="id")
    old_df = old_df.drop(columns="price_y")
    old_df = old_df.rename(columns={"price_x": "price"})

    
    for i in range(len(to_scrape)): # 
        data = to_scrape.iloc[i]
        url = data["link"]
        print(url)
        listing_html = request_to_html(url, scraper)
        print(f"currently scraping listing {i+1} out of {len(to_scrape)} with link {url}")
        if safe_mode:
            try:
                info = parse_listing_new(listing_html)
            except:
                print(f"Error scraping result {i} with link {to_scrape.iloc[i]['link']}")
                continue
        else:
            info = parse_listing_new(listing_html)

        row = to_scrape.iloc[i]

        to_add = dict(zip(
                old_df.columns, 
                # [to_scrape.iloc[i]["link"]] + [to_scrape.iloc[i]["price"]]+ info + [now] + [True] + [to_scrape.iloc[i]["id"]] + [to_scrape.iloc[i]["price"]]
                [row["id"], row["price"], row["link"], row["price"]] + info + [True]
            ))
        
        old_df = old_df.append(
            to_add,
        ignore_index=True)

        old_df.to_csv(os.path.join(DATA_PATH,f"{id_}_scraping_results_.csv"))



if __name__ == "__main__":
    current_time = datetime.datetime.now().strftime("%Y-%m-%dT%H")
    scraper = cloudscraper.create_scraper()

    main(
        "immowelt\\data",
        "https://www.immowelt.de/liste/dresden/wohnungen/kaufen?d=true&sd=DESC&sf=RELEVANCE&sp=",
        True
    )


    

    
    #df_merged = df_new[["id", "price"]].merge(df_old, how="right", right_on="id", left_on="id")
    # df_merged.to_csv("merged.csv")

    

    