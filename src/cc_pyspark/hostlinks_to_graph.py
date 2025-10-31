import idna
import logging
import os

from sparkcc import CCSparkJob
from pyspark.sql import functions as sqlf
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from iana_tld import iana_tld_list
from wat_extract_links import ExtractHostLinksJob


class HostLinksToGraph(CCSparkJob):
    """Construct host-level webgraph from table with link pairs
    <from_host, to_host> (input is a table with reversed host names)."""

    name = "LinksToGraph"

    def add_arguments(self, parser):
        parser.add_argument("--save_as_text", type=str, default=None,
                            help="Save webgraph also as text on path")
        parser.add_argument("--normalize_host_names", action='store_true',
                            help="Normalize host names: replace Unicode IDNs"
                            " by their ASCII equivalents")
        parser.add_argument("--validate_host_names", action='store_true',
                            help="Validate host names and skip vertices with"
                            " invalid name during assignment of vertex IDs")
        parser.add_argument("--vertex_partitions", type=int, default=1,
                            help="Number of partitions to enumerate and store"
                            " vertices. The default (1 partition) is recommended"
                            " only for smaller graphs. IDs are represented"
                            " either as int (if 1 partition) or long (multiple"
                            " partitions).")
        parser.add_argument("--vertex_ids", type=str,
                            help="Path to table providing hostname - vertex ID"
                            " mappings. If the mapping exists IDs are read"
                            " from it, otherwise the mapping is created and"
                            " also saved as table.")
        parser.add_argument("--add_input", type=str, action='append',
                            help="Additional input table to be merged")

    @staticmethod
    def reverse_host(host):
        parts = host.split('.')
        parts.reverse()
        return '.'.join(parts)

    @staticmethod
    def reverse_host_is_valid(rev_host):
        if rev_host is None:
            return False
        if '.' not in rev_host:
            return False
        # fast check for valid top-level domain
        # (modules tldextract and tld are too expensive)
        tld = rev_host.split('.')[0]
        if tld not in iana_tld_list:
            logging.debug("No valid tld {} in {}".format(tld, rev_host))
            return False
        return True

    @staticmethod
    def reverse_host_normalize(rev_host):
        parts = rev_host.split('.')
        modified = False
        for (i, part) in enumerate(parts):
            if not ExtractHostLinksJob.host_part_pattern.match(part):
                try:
                    idn = idna.encode(part).decode('ascii')
                    parts[i] = idn
                    modified = True
                except (idna.IDNAError, idna.core.InvalidCodepoint, UnicodeError, IndexError, Exception):
                    return None
        if modified:
            return '.'.join(parts)
        return rev_host

    def vertices_assign_ids(self, session, edges):
        source = edges.select(edges.s.alias('name'))
        target = edges.select(edges.t.alias('name'))

        ids = source.union(target) \
            .distinct()

        if self.args.normalize_host_names:
            normalize = sqlf.udf(HostLinksToGraph.reverse_host_normalize,
                                 StringType())
            ids = ids.withColumn('name', normalize(ids['name']))
            ids = ids.dropna().distinct()

        if self.args.validate_host_names:
            is_valid = sqlf.udf(HostLinksToGraph.reverse_host_is_valid,
                                BooleanType())
            ids = ids.filter(is_valid(ids['name']))

        if self.args.vertex_partitions == 1:
            ids = ids \
                    .coalesce(1) \
                    .sort('name') \
                    .withColumn('id', sqlf.monotonically_increasing_id())
        else:
            id_rdd = ids.select(ids.name).rdd \
                        .map(lambda row: tuple(row)[0]) \
                        .sortBy(lambda x: x, True,
                                self.args.vertex_partitions) \
                        .zipWithIndex()
            id_schema = StructType([
                StructField("name", StringType(), True),
                StructField("id", LongType(), True)
            ])
            ids = session.createDataFrame(id_rdd, schema=id_schema)

        if self.args.save_as_text is not None:
            ids = ids.persist()
            ids.select(sqlf.concat_ws('\t', ids.id, ids.name)) \
                .write \
                .text(os.path.join(self.args.save_as_text, "vertices"),
                      compression="gzip")
        ids.write \
           .format(self.args.output_format) \
           .option("compression", self.args.output_compression) \
           .saveAsTable(self.args.output + '_vertices')

        return ids

    def run_job(self, session):

        # read edges  s -> t  (host names)
        edges = session.read.load(self.args.input)

        if self.args.add_input:
            # merge multiple input graphs
            for add_input in self.args.add_input:
                add_edges = session.read.load(add_input)
                edges = edges.union(add_edges)

            # remove duplicates and sort
            edges = edges \
                .dropDuplicates() \
                .sortWithinPartitions('s', 't')

        if self.args.vertex_ids is not None:
            ids = session.read.load(self.args.vertex_ids)
        else:
            ids = self.vertices_assign_ids(session, edges)

        edges = edges.join(ids, edges.s == ids.name, 'inner')
        edges = edges.select(edges.id.alias('s'), 't')
        edges = edges.join(ids, edges.t == ids.name, 'inner')
        edges = edges.select('s', edges.id.alias('t'))
        edges = edges \
            .coalesce(self.args.num_output_partitions) \
            .sortWithinPartitions('s', 't')

        # remove self-loops
        # (must be done after assignment of IDs so that isolated
        # nodes/vertices are contained in map <name, id>
        edges = edges.filter(edges.s != edges.t)

        if self.args.save_as_text is not None:
            edges = edges.persist()
            edges.select(sqlf.concat_ws('\t', edges.s, edges.t)) \
                 .write \
                 .text(os.path.join(self.args.save_as_text, "edges"),
                       compression="gzip")
            # TODO: save as adjacency list
        edges.write \
             .format(self.args.output_format) \
             .option("compression", self.args.output_compression) \
             .saveAsTable(self.args.output + '_edges')


if __name__ == "__main__":
    job = HostLinksToGraph()
    job.run()
