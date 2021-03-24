from __future__ import annotations


class Backend:
    """ Backend represents a storage for internal Artigraph metadata.

        Backend storage is an addressable location (local path, database connection, etc) that
        tracks metadata for a collection of Graphs over time, including:
        - the Artifact(s)->Producer->Artifact(s) dependency graph
        - Artifact Annotations, Statistics, Partitions, and other metadata
        - Artifact and Producer Fingerprints
        - etc
    """
