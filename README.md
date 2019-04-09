# Neo4j Time-series

A simple library for creating and updating a time-series using Neo4j using
[swinging door compression](https://support.industry.siemens.com/cs/document/109739594/compressing-of-process-value-archives-with-the-swinging-door-algorithm-in-pcs-7?dti=0&lc=en-TR)

Note that this library currently discards uncompressed samples.


## Example

`examples/sine_wave.py` holds an example where a sine-wave is used to generate some dummy-sensor data.
