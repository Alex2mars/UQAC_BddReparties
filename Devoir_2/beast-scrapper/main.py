import requests
import lxml.html
import re
import json

html = requests.get("https://www.d20pfsrd.com/bestiary/monster-listings/")
doc = lxml.html.fromstring(html.content)

links = doc.xpath("//a")

monster_links = []

for link in links:
    if "href" in link.attrib:
        href = link.attrib["href"]
        if re.search(".*monster-listings/[a-z]+/[a-z]+/?", href):
            if "templates/" not in href:
                monster_links.append(href)
py_json_res = []

i = 0
for monster_link in monster_links:
    total = len(monster_links)
    print(str(i) + "/" + str(total), str(round(i / total * 100, ndigits=2)) + "%")
    i += 1
    monster_html = requests.get(monster_link)
    monster_doc = lxml.html.fromstring(monster_html.content)

    subpages = monster_doc.xpath('//b[text()="Subpages"]')
    if subpages:
        sub_links = monster_doc.xpath('//li[@class="page new parent"]/a')
        for sub_link in sub_links:
            monster_links.append(sub_link.attrib["href"])
        continue

    # Get name
    name = monster_doc.xpath("//p[@class='title']/text()")
    if len(name) > 0:
        name = name[0].strip()
    else:
        name = monster_doc.xpath('//span[@class="level"]/ancestor::*[1]/text()')
        if len(name) < 1:
            name = monster_link[:-1].rsplit('/', 1)[-1]
            name = " ".join(name.split("-")).title()
        else:
            name = name[0].strip()

    res_dict = {"name": name, "spells": [], "link": monster_link}

    # Get spells
    spells = monster_doc.xpath('//a[@class="spell"]')
    for spell in spells:
        href = spell.attrib["href"]
        spell_name = href.rsplit('/', 1)[-1]
        spell_name = " ".join(spell_name.split("-")).title()
        res_dict["spells"].append(spell_name)

    py_json_res.append(res_dict)

file = open("beast.json", "w")
str_json = json.dumps(py_json_res)
file.write(str_json)
file.close()

