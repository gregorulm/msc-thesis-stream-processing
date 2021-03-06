Master's thesis project in Computer Science  
Gregor Ulm  
Department of Computer Science and Engineering  
Chalmers University of Technology

Latency and Throughput in Center versus Edge Stream Processing: A Case Study in the Transportation Domain  


Abstract

The emerging Internet of Things (IoT) enables novel solutions. In this thesis report, we turn our attention to the problem of providing targeted accident notifications in near real-time. We use traffic data generated by Linear Road, a popular benchmark for stream processing engines, and simulate a possible real-world scenario in which connected cars continuously send position updates. We analyze this stream of position updates with the goal of identifying accidents, so that targeted accident notifications can be issued. This means that only cars within a certain distance of a known accident site will be notified.

In a real-world scenario, the required data analysis could be performed in different ways. We consider two possibilities. First, position reports are aggregated by road side units (RSUs) and forwarded to a central server. Afterwards, the results are sent back to the cars, again involving RSUs for transmission. We refer to this as center stream processing. Second, all data analysis is performed on RSUs. An RSU is less powerful than a server. However, RSUs are located much closer to the cars than a central server. We refer to this case as edge stream processing. Performing computations directly on RSUs has the benefit that the cost of the roundtrip time for data transmission from RSUs to the server and back will be avoided. We use a contemporary stream processing engine for data analysis, and compare latency and throughput of an implementation of our solution to the accident notification problem in both cases.