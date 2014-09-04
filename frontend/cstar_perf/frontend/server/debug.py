from werkzeug.wrappers import Request, Response
from werkzeug.debug import DebuggedApplication

@Request.application
def app(request):
    raise ValueError("testing debugger")
    return Response("hello, world!")

app = DebuggedApplication(app, evalex=True)

from werkzeug.serving import run_simple
run_simple("localhost", 4000, app)
