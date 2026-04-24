import React, { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { getPersonaUseCases } from "../api";

const VIZ_LABELS = {
  graph: "Graph",
  table: "Table",
  timeline: "Timeline",
  tree: "Tree",
  pie: "Chart",
  grid: "Grid",
  dashboard: "Dashboard",
  bar: "Chart",
};

export default function PersonaPage() {
  const { personaId } = useParams();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    getPersonaUseCases(personaId)
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [personaId]);

  if (loading) return <div className="loading">Loading...</div>;
  if (!data) return <div className="loading">Persona not found</div>;

  const { persona, use_cases } = data;

  return (
    <div>
      <div className="page-header">
        <div className="breadcrumb">
          <Link to="/">Home</Link>
          <span>/</span>
          <span>{persona.name}</span>
        </div>
        <h1>{persona.name}</h1>
        <p>{persona.description}</p>
      </div>

      <div className="uc-list">
        {use_cases.map((uc) => (
          <Link
            key={uc.id}
            to={`/usecase/${uc.id}`}
            style={{ textDecoration: "none", color: "inherit" }}
          >
            <div className="uc-item">
              <span className="uc-number">UC{String(uc.id).padStart(2, "0")}</span>
              <span className="uc-name">{uc.name}</span>
              <span className="uc-viz-badge">
                {VIZ_LABELS[uc.viz] || uc.viz}
              </span>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
