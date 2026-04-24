import React, { useMemo } from "react";
import ForceGraph2D from "react-force-graph-2d";

export default function Graph({ nodes, links, width, height }) {
  const data = useMemo(() => {
    if (!nodes || !links) return { nodes: [], links: [] };
    return {
      nodes: nodes.map((n) => ({ ...n })),
      links: links.map((l) => ({ ...l })),
    };
  }, [nodes, links]);

  if (data.nodes.length === 0) {
    return <div className="loading">No graph data</div>;
  }

  return (
    <div
      style={{
        background: "var(--surface)",
        borderRadius: 16,
        border: "1px solid var(--border)",
        overflow: "hidden",
      }}
    >
      <ForceGraph2D
        graphData={data}
        width={width || 900}
        height={height || 500}
        nodeLabel="label"
        nodeColor={(n) => n.color || "#1fd9fe"}
        nodeRelSize={6}
        linkColor={() => "rgba(31,217,254,0.3)"}
        linkDirectionalArrowLength={4}
        linkDirectionalArrowRelPos={1}
        backgroundColor="#0a1e3d"
        nodeCanvasObjectMode={() => "after"}
        nodeCanvasObject={(node, ctx, globalScale) => {
          const label = node.label || node.id;
          const fontSize = 11 / globalScale;
          ctx.font = `${fontSize}px Inter, sans-serif`;
          ctx.textAlign = "center";
          ctx.textBaseline = "middle";
          ctx.fillStyle = "#e8edf3";
          ctx.fillText(label, node.x, node.y + 10);
        }}
      />
    </div>
  );
}
