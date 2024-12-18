import rdflib
import pathlib
from rdflib_hdt import HDTStore

outdir = pathlib.Path('cache/test')
outdir.mkdir(parents=True, exist_ok=True)

# Path to the HDT file
hdt_file = pathlib.Path('brick/annotations.hdt')

try:
    # Load the HDT file into an RDFLib graph
    store = HDTStore(hdt_file.as_posix())
    graph = rdflib.Graph(store=store)

    # Count the number of triples
    triple_count = sum(1 for _ in graph.triples((None, None, None)))

    # Generate metadata
    metadata = {
        "triple_count": triple_count,
        "namespaces": list(graph.namespaces()),
        "sample_triples": list(graph.triples((None, None, None)))[:5]  # Limit to first 5 triples
    }

    # Write metadata to a file
    metadata_file = outdir / "test.txt"
    with open(metadata_file, "w") as f:
        f.write(f"Triple Count: {metadata['triple_count']}\n")
        f.write("Namespaces:\n")
        for prefix, uri in metadata['namespaces']:
            f.write(f"  {prefix}: {uri}\n")
        f.write("Sample Triples:\n")
        for s, p, o in metadata['sample_triples']:
            f.write(f"  {s} {p} {o}\n")

    print(f"Metadata written to {metadata_file}")

except Exception as e:
    # Explicitly fail if the graph fails to load
    print(f"Failed to parse the graph: {e}")
    raise