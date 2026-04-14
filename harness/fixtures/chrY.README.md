# chrY fixture (not committed)

The chrY HPRC fixture is too large for git (~12 MB gzipped). To enable the
chrY test locally, symlink it into this directory:

```bash
ln -sf <path/to/chrY.hprc-v1.1-mc-grch38.gfa.gz> harness/fixtures/chrY.gfa.gz
```

With pangyplot checked out alongside this repo:

```bash
ln -sf \
  ../../../pangyplot/pangyplot/datastore/graphs/hprc.prepared/chrY/chrY/chrY.hprc-v1.1-mc-grch38.gfa.gz \
  harness/fixtures/chrY.gfa.gz
```

First run decompresses it to `harness/fixtures/chrY.gfa` (cached).

## Running chrY

```bash
python -m harness.run --gfa harness/fixtures/chrY.gfa.gz --record-stats
pytest tests/ -m slow
```

chrY is a perf canary, not a golden fixture — it takes ~1–2 min with the
current pangyplot-optimizations patches and doesn't produce a committed
snapshot.
