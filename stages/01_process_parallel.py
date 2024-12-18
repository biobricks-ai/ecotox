import biobricks as bb
import pandas as pd
import pyarrow.parquet as pq
import json
import pathlib
import shutil
from tqdm import tqdm
from rdflib import Graph, Literal, Namespace, RDF, URIRef
import subprocess
from multiprocessing import Pool

def process_single_batch(args):
    """
    Process a single batch of data into Turtle and HDT files.
    """
    batch_num, batch, namespaces, cachedir = args

    # Convert the Arrow table batch to a pandas DataFrame
    batch_df = batch.to_pandas()

    # Create a new RDF graph
    g = Graph()

    # Bind namespaces
    for key, val in namespaces.items():
        g.bind(key, val)

    # Iterate over rows in the batch DataFrame
    for index, row in batch_df.iterrows():
        cid = row['EcotoxCID']
        sid = row['EcotoxSID']
        anid = row['ANID']

        # Create URIs
        annotation_iri = URIRef(namespaces["ecotoxannotation"] + f"ANID{anid}")
        compound_iri = [URIRef(namespaces["ecotoxcompound"] + f"CID{c}") for c in cid]
        substance_iri = [URIRef(namespaces["ecotoxsubstance"] + f"CID{s}") for s in sid]

        # Parse the data field as JSON
        data = json.loads(row['Data'])
        # annotation may have multiple values
        string_with_markup_list = [markup.get('String', '') for markup in data.get('Value', {}).get('StringWithMarkup', [])]

        # Add triples to the graph
        g.add((annotation_iri, RDF.type, namespaces["oa"].Annotation))

        # Add the CID to the annotation
        for ci in compound_iri:
            g.add((annotation_iri, namespaces["oa"].hasTarget, ci))
            g.add((annotation_iri, namespaces["dc"].subject, ci))

        # Add the SID to the annotation
        for si in substance_iri:
            g.add((annotation_iri, namespaces["oa"].hasTarget, si))
            g.add((annotation_iri, namespaces["dc"].subject, si))

        # Add body
        body = URIRef(f"{annotation_iri}/body")
        g.add((annotation_iri, namespaces["oa"].hasBody, body))
        for swm in string_with_markup_list:
            g.add((body, RDF.value, Literal(swm)))

        g.add((body, namespaces["dc"]["format"], Literal("text/plain")))

    # Serialize the graph to Turtle format
    turtle_file = str(cachedir / f"annotations_{batch_num}.ttl")
    g.serialize(destination=turtle_file, format='turtle')

    # Convert the Turtle file into an HDT file
    hdt_file = str(cachedir / f"annotations_{batch_num}.hdt")
    subprocess.run(["rdf2hdt.sh", "-rdftype", "turtle", turtle_file, hdt_file], check=True)

    return None
    
# Instead of listing all batches, use a generator function
def batch_generator(rawpa, batch_size, namespaces, cachedir):
    for i, batch in enumerate(rawpa.iter_batches(batch_size), start=1):
        yield (i, batch, namespaces, cachedir)

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

## Use a Pool and imap to process batches in parallel
#with Pool() as pool:
#    # Pass the generator directly to pool.imap
#    for _ in tqdm(
#        pool.imap(
#            process_single_batch,
#            batch_generator(rawpa, batch_size, namespaces, cachedir)
#        ), 
#        total = n_batch,
#        desc="Processing batches"
#    ):
#        pass

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
