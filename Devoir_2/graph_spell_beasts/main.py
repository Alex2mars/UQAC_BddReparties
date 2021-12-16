import networkx as nx
import json

spell = "Sort"
creature_list = "Liste des créatures"

with open("data_spells_beasts.json", encoding="utf-8") as data_file:
    data = json.load(data_file)

    graph = nx.DiGraph()

    for entry in data:
        sort = entry[spell]

        graph.add_node(sort)
        graph.nodes[sort]["viz"] = {'color': {'r': 0, 'g': 255, 'b': 0, 'a': 0}}

        creatures = entry[creature_list]
        for creature in creatures:
            graph.add_edge(sort, creature)
            graph.nodes[creature]["viz"] = {'color': {'r': 255, 'g': 0, 'b': 0, 'a': 0}}

    nx.write_gexf(graph, "graph.gexf")
