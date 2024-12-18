import biobricks as bb
import pandas as pd
import pyarrow.parquet as pq
import json
import pathlib
import shutil
from tqdm import tqdm
from rdflib import Graph, Literal, Namespace, RDF, URIRef
import subprocess

tqdm.pandas()

# cachedir for ttl files, if needed
cachedir = pathlib.Path('cache/process')
cachedir.mkdir(parents=True, exist_ok=True)
# remove unneeded files after processing

# outdir should be brick (hdt file only)
outdir = pathlib.Path('./brick')
outdir.mkdir(parents=True, exist_ok=True)

print('Reading annotations ...')
pa_brick = bb.assets('ecotox')
print("Done reading annotations")
print(pa_brick.annotations_parquet)
# pa_brick has a single table `annotations_parquet`
# use pyarrow to read the parquet file in chunks
rawpa = pq.ParquetFile(pa_brick.annotations_parquet)
n_row = rawpa.metadata.num_rows
print(f"Number of rows: {n_row}")

# get row0 and make it json for a pretty print
row_group0 = rawpa.read_row_group(0).to_pandas()
row0 = row_group0.iloc[0]
print(json.dumps(row0.apply(str).to_dict(), indent=4))



# Define namespaces
namespaces_sources = {
    "rdf" : "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "ecotoxcompound" : "http://rdf.ncbi.nlm.nih.gov/ecotox/compound/",
    "ecotoxsubstance" : "http://rdf.ncbi.nlm.nih.gov/ecotox/substance/",
    "ecotoxannotation" : "http://rdf.ncbi.nlm.nih.gov/ecotox/annotation/",
    "oa" : "http://www.w3.org/ns/oa#",
    "dc" : "http://purl.org/dc/elements/1.1/",
}

namespaces = {key: Namespace(val) for key, val in namespaces_sources.items()}

''' VISUAL REPRESENTATION OF GRAPH COMPONENT
*markup is short for string_with_markup

                         +------------------+
                 +-------|  annotation_iri  |
                 |       +------------------+
                 |             |      |      
            RDF.type           |   OA.hasBody       
                 |             |      |      
                 v             |      |  +-------------------+
       +-----------------+     |      +->|       body        |
       |  OA.Annotation  |     |         +-------------------+
       +-----------------+     |            |               |
                               |         RDF.value      DC["format"]
                               |            |               |
                               |            v               v
                               |  +-----------------+ +-----------------------+
                               |  | Literal(markup) | | Literal("text/plain") |
                               |  +-----------------+ +-----------------------+
                               |
                  OA.hasTarget / DC.subject
                               |
                  +---------+--o--------------+-------------+
                  |         |                 |             |
                  v         |                 v             |
	  +-------------------+ |        +-------------------+  |
	  |   compound_iri_1  | |  ...   |   compound_iri_m  |  | 
	  +-------------------+ |        +-------------------+  |
	                        v                               v
				  +-------------------+          +-------------------+ 
				  |  substance_iri_1  |   ...    |  substance_iri_n  |   
				  +-------------------+          +-------------------+
'''

batch_size = 10000
n_batch = n_row // batch_size + 1
batch_num = -1
# loop through rawpa, creating a chemical for each row
for batch in tqdm(rawpa.iter_batches(batch_size), total = n_batch, desc = "Processing batches"):
    batch_num += 1
    batch_df = batch.to_pandas()
    # Create a new RDF graph
    g = Graph()
    # Bind namespaces
    for key, val in namespaces.items():
        g.bind(key, val)

    for index, row in batch_df.iterrows():
        cid = row['EcotoxCID']
        sid = row['EcotoxSID']
        anid = row['ANID']

        # Create URIs
        annotation_iri = URIRef(namespaces["ecotoxannotation"] + f"ANID{anid}")
        compound_iri = [URIRef(namespaces["ecotoxcompound"] + f"CID{c}") for c in cid]
        substance_iri = [URIRef(namespaces["ecotoxsubstance"] + f"CID{s}") for s in sid]

        # create the value for the annotation
        # # Parse the Data Field as JSON
        data = json.loads(row['Data'])
        # # annotation may have multiple values
        string_with_markup_list = [markup.get('String', '') for markup in data.get('Value', {}).get('StringWithMarkup', [])]

        # add triples to the graph
        g.add((annotation_iri, RDF.type, namespaces["oa"].Annotation))

        # add the CID to the annotation, skip if there are no CIDs
        for ci in compound_iri:
            g.add((annotation_iri, namespaces["oa"].hasTarget, ci))
            g.add((annotation_iri, namespaces["dc"].subject, ci))

        # add SID to the annotation, skip if there are no SIDs
        for si in substance_iri:
            g.add((annotation_iri, namespaces["oa"].hasTarget, si))
            g.add((annotation_iri, namespaces["dc"].subject, si))

        body = URIRef(f"{annotation_iri}/body")
        g.add((annotation_iri, namespaces["oa"].hasBody, body))
        # triple quotes used to allow multi-line strings
        for swm in string_with_markup_list:
            g.add((body, RDF.value, Literal(swm)))

        g.add((body, namespaces["dc"]["format"], Literal("text/plain")))

    # Serialize the graph to a string in Turtle format
    turtle_file = str(cachedir / f"annotations_{batch_num}.ttl")
    g.serialize(destination=turtle_file, format='turtle')

    # Convert the Turtle file into an HDT file
    hdt_file = str(cachedir / f"annotations_{batch_num}.hdt")
    subprocess.run(["rdf2hdt.sh", "-rdftype", "turtle", turtle_file, hdt_file], check=True)
    

print("Combining HDT files ...")
hdt_combined = str(outdir / 'annotations.hdt')
subprocess.run(
    [
        "hdtCat.sh",
        str(cachedir) + "/annotations_*.hdt",
        hdt_combined
    ],
    check=True
)
print(f"Done writing HDT file to {hdt_file}")

# delete cache directory
shutil.rmtree(pathlib.Path('cache'))
