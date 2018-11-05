# ERDOS Tests

#### Test basic sender/receiver connection
```commandline
python communication_patterns_test --framework={FRAMEWORK} --case={NUM_PUB}-{NUM_SUB}
```
FRAMEWORK = ray, ros
NUM_PUB, NUM_SUB = any positive integer

#### Test loop
```commandline
python control_loop_test --framework={FRAMEWORK}
```

#### Test benchmark Pylot
```commandline
python benchmark/pylot_benchmark_test.py --framework={FRAMEWORK}
```

