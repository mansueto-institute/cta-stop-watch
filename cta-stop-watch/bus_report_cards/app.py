from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from pathlib import Path


app = FastAPI()

app.mount(
    "/static",
    StaticFiles(directory=Path(__file__).parent / "static"),
    name="static",
)

templates = Jinja2Templates(directory="templates")

# fastapi dev app.py


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


@app.get("/home", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


@app.get("/methods", response_class=HTMLResponse)
async def methods(request: Request):
    return templates.TemplateResponse("methods.html", {"request": request})


@app.get("/report", response_class=HTMLResponse)
async def report(request: Request):
    return templates.TemplateResponse("report.html", {"request": request})


@app.get("/people", response_class=HTMLResponse)
async def people(request: Request):
    return templates.TemplateResponse("people.html", {"request": request})


@app.get("/analysis", response_class=HTMLResponse)
async def analysis(request: Request):
    return templates.TemplateResponse("analysis.html", {"request": request})


@app.get("/assets", response_class=HTMLResponse)
async def assets(request: Request):
    return templates.TemplateResponse("assets.html", {"request": request})
