import sys, time
from pypandas.datasets import *
from pypandas.outlier import *

starttime = time.time()

remover_type = sys.argv[1]
num_features = int(eval(sys.argv[2]))
clusters = int(eval(sys.argv[3]))

df = load_data_job("aws")
columns = ["Job #", "Doc #", "Bin #", "Initial Cost", "Total Est Fee", "Existing Zoning Sqft", "Proposed Zoning Sqft", "Enlargement SQ Footage", "Street Frontage", "ExistingNo of Stories", "Proposed No of Stories", "Existing Height", "Proposed Height"]

outlier_remover = OutlierRemover.factory(remover_type)
outlier_remover.set_param(k = clusters)
outlier_remover.fit(df, columns[:num_features])
s = outlier_remover.summary()
s.show()

print("Remover type: {0:s}, # features: {1:d}, k: {2:d}".format(remover_type, num_features, clusters))
print("Time: " + str(time.time() - starttime))

