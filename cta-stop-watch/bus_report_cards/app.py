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


@app.get("/about", response_class=HTMLResponse)
async def about(request: Request):
    return templates.TemplateResponse("about.html", {"request": request})


@app.get("/report", response_class=HTMLResponse)
async def report(request: Request):
    return templates.TemplateResponse("report.html", {"request": request})
