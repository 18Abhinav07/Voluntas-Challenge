# Sample capture

Two minutes of live Polymarket, recorded by the collector on the AWS run of 24 October 2026
(c7i.xlarge in eu-west-2, build 7170677; see [docs/PERFORMANCE.md](../docs/PERFORMANCE.md)).

| File | What it is |
|---|---|
| `ob.sbe.gz` | The SBE output, gzipped. 500 markets, 1,962 order books, 119.5 s: 5,912 snapshots and 6,926 level updates |
| `collector.log` | The collector's stderr for that run: discovery, status lines, the `done` totals |

The file is 64 MiB unpacked because the writer emits whole 64 MiB aligned chunks (D11); most
of it is padding, which is why it gzips to under 600 KB.

To check it without a network connection:

```bash
./demo.sh sample
```

Or by hand:

```bash
gunzip -k sample/ob.sbe.gz && ./target/release/verify sample/ob.sbe
```

The expected result is `3950 snapshots matched the rebuilt book, 0 did not` and `OK`.
