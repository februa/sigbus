# Endpoints delegate to storage; they do not depend on the memory representation.
from ..inprocess.endpoint import InProcessReadEndpoint as SharedMemoryReadEndpoint
from ..inprocess.endpoint import InProcessWriteEndpoint as SharedMemoryWriteEndpoint
