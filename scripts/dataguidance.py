#!/usr/bin/env python3
# vim: set ts=4 sw=4 ts=4 et :

import argparse
import logging
import time

import bs4
import pandas as pd  # type: ignore
import requests
import ipdb  # type: ignore

logging.basicConfig(level=logging.DEBUG)
log: logging.Logger = logging.getLogger()

logging.getLogger("urllib3").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Default")
    parser.add_argument("--outfile", help="output filename", default="dataguidance.csv")
    parser.add_argument("--metadata", help="metadata file", default="../metadata.csv")
    parser.add_argument("--debug", help="debug", action="store_true")
    return parser.parse_args()


def get_page_text(url: str) -> str:
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to GET {url}")
        raise e
    return response.text


def get_soup_from_url(url: str) -> bs4.BeautifulSoup:
    try:
        source: str = get_page_text(url)
        soup: bs4.BeautifulSoup = bs4.BeautifulSoup(source, "html.parser")
    except Exception as e:
        raise e
    return soup


def main() -> None:
    args: argparse.Namespace = parse_args()

    log.info("Running {}".format(__file__))
    if args.debug:
        log.setLevel(logging.DEBUG)
        log.debug("Debug mode enabled")

    # profiling
    start_time: float = time.perf_counter()

    URL_TEMPLATE: str = "https://www.dataguidance.com/jurisdiction/{state_name}"

    metadata: pd.DataFrame = pd.read_csv(args.metadata)

    for state_name in metadata["state_name"]:
        log.info(f"Scraping {state_name}")

        # normalize state name
        state_name_norm = state_name.replace(" ", "-").lower()
        log.debug(f"Normalized name: {state_name_norm}")

        url: str = URL_TEMPLATE.format(state_name=state_name_norm)
        try:
            soup: bs4.BeautifulSoup = get_soup_from_url(url)
        except:
            log.warning(f"Could not fetch data for {state_name}")
            continue

        # fetch paragraphs from summary
        try:
            summary: bs4.element.ResultSet = (
                soup.find("h2", text="Summary")  # type: ignore
                .find_next("div", class_="field-content")
                .find_all("p")
            )
        except:
            log.warning("Failed to parse {url}")
            continue

        # get index of state in metadata dataframe
        state_loc: pd.Series = metadata["state_name"] == state_name

        # fill the regulator url
        try:
            regulator_url: str = summary[1].a["href"]
            metadata.loc[state_loc, ["regulator_url"]] = regulator_url
        except:
            log.warning("Could not fetch regulator")

        try:
            # get regulation if relevant
            regulation_url: str = summary[0].a["href"]
            if "usa-state-law-tracker" not in regulation_url:
                # check if it's a direct link
                if "dataguidance.com" not in regulation_url:
                    metadata.loc[state_loc, ["regulation_url"]] = regulation_url
                else:
                    # get subpage for regulation and fetch the direct url
                    soup = get_soup_from_url(regulation_url)
                    regulation_url = soup.find_all("a", text="View")[0]["href"]
                    metadata.loc[state_loc, ["regulation_url"]] = regulation_url
        except:
            log.warning("Could not fetch regulation")

        log.info(metadata.loc[state_loc].to_dict())

        metadata.to_csv(args.outfile, index=False)

    elapsed_time: float = time.perf_counter() - start_time
    log.info(f"{__file__} executed in {elapsed_time:0.5f} seconds.")


if __name__ == "__main__":
    main()
