# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import vacantes

app = FastAPI(title="Servicio de Empleo API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173",
                  "https://servicio-publico-empleo-prototipo-f.vercel.app"],  # puerto de Vite/React
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(vacantes.router)

@app.get("/")
def root():
    return {"status": "ok", "mensaje": "API Servicio de Empleo activa"}


# Para ejecutar el proyecto damos uvicorn main:app --reload estando en app por el lado del backend
# Para el frontend estando en frontend damos npm run dev para iniciar el servidor de desarrollo de Vite/React.
