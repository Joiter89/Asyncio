import asyncio
import datetime
from tqdm import tqdm
import aiohttp
import requests
from more_itertools import chunked

from models import Base, SwapiPeople, Session, engine

MAX_REQUESTS_CHUNK = 5


def get_data(url_list):
    final_list = []
    for url in url_list:
        info_json = requests.get(url).json()
        first_key = list(info_json.keys())[0]
        final_list.append(info_json[first_key])
    return final_list


async def insert_people(people_list_json):
    people_list = [SwapiPeople(
        birth_year=person["birth_year"],
        eye_color=person["eye_color"],
        films=' , '.join(get_data(person["films"])),
        gender=person["gender"],
        hair_color=person["hair_color"],
        height=person["height"],
        homeworld=person["homeworld"],
        mass=person["mass"],
        name=person["name"],
        skin_color=person["skin_color"],
        species=' , '.join(get_data(person["species"])),
        starships=' , '.join(get_data(person["starships"])),
        vehicles=' , '.join(get_data(person["vehicles"]))
    ) for person in tqdm(people_list_json) if person.get("birth_year")]
    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


async def get_people(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.py4e.com/api/people/{people_id}")
    json_data = await response.json()
    await session.close()
    return json_data


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    for person_ids_chunk in chunked(range(1, 100), MAX_REQUESTS_CHUNK):
        persons_coro = []
        for person_id in person_ids_chunk:
            person_coro = get_people(person_id)
            if person_coro is not None:
                persons_coro.append(person_coro)
        people = await asyncio.gather(*persons_coro)
        insert_people_coro = insert_people(people)
        asyncio.create_task(insert_people_coro)

    main_task = asyncio.current_task()
    insets_task = asyncio.all_tasks() - {main_task}
    await asyncio.gather(*insets_task)


if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)