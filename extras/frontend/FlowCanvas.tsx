import { useCallback, useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";
import { Maximize2, Minus, Plus } from "lucide-react";

export function FlowCanvas({ children }: { children: ReactNode }) {
  const viewport = useRef<HTMLDivElement>(null);
  const world = useRef<HTMLDivElement>(null);
  const drag = useRef<{ x: number; y: number } | null>(null);
  const [view, setView] = useState({ x: 0, y: 0, scale: 1 });
  const fit = useCallback(() => {
    const box = viewport.current,
      content = world.current;
    if (!box || !content) return;
    const scale = Math.min(
      (box.clientWidth - 32) / 1100,
      (box.clientHeight - 72) / content.offsetHeight,
      1,
    );
    setView({
      x: (box.clientWidth - 1100 * scale) / 2,
      y: Math.max(
        16,
        (box.clientHeight - 40 - content.offsetHeight * scale) / 2,
      ),
      scale,
    });
  }, []);
  const zoom = useCallback((factor: number, x: number, y: number) => {
    setView((previous) => {
      const scale = Math.max(0.2, Math.min(2.5, previous.scale * factor));
      const ratio = scale / previous.scale;
      return {
        scale,
        x: x - (x - previous.x) * ratio,
        y: y - (y - previous.y) * ratio,
      };
    });
  }, []);
  useEffect(() => {
    const box = viewport.current,
      content = world.current;
    if (!box || !content) return;
    const observer = new ResizeObserver(fit);
    observer.observe(box);
    observer.observe(content);
    const wheel = (event: WheelEvent) => {
      event.preventDefault();
      const rect = box.getBoundingClientRect();
      zoom(
        Math.exp(-Math.max(-500, Math.min(500, event.deltaY)) * 0.0015),
        event.clientX - rect.left,
        event.clientY - rect.top,
      );
    };
    box.addEventListener("wheel", wheel, { passive: false });
    return () => {
      observer.disconnect();
      box.removeEventListener("wheel", wheel);
    };
  }, [fit, zoom]);
  function centerZoom(factor: number) {
    const box = viewport.current;
    if (box) zoom(factor, box.clientWidth / 2, box.clientHeight / 2);
  }
  return (
    <div
      className="flow-canvas"
      ref={viewport}
      role="region"
      aria-label="Flow canvas"
      tabIndex={0}
      onKeyDown={(event) => {
        if (event.target !== event.currentTarget) return;
        if (event.key === "+" || event.key === "=") {
          event.preventDefault();
          centerZoom(1.2);
        }
        if (event.key === "-") {
          event.preventDefault();
          centerZoom(1 / 1.2);
        }
        if (event.key === "0") {
          event.preventDefault();
          fit();
        }
        const delta: Record<string, [number, number]> = {
          ArrowLeft: [30, 0],
          ArrowRight: [-30, 0],
          ArrowUp: [0, 30],
          ArrowDown: [0, -30],
        };
        const offset = delta[event.key];
        if (offset) {
          event.preventDefault();
          setView((v) => ({ ...v, x: v.x + offset[0], y: v.y + offset[1] }));
        }
      }}
      onPointerDown={(event) => {
        if (
          event.button !== 0 ||
          (event.target as HTMLElement).closest("button,input,select")
        )
          return;
        drag.current = { x: event.clientX, y: event.clientY };
        event.currentTarget.setPointerCapture(event.pointerId);
      }}
      onPointerMove={(event) => {
        const start = drag.current;
        if (!start) return;
        const dx = event.clientX - start.x,
          dy = event.clientY - start.y;
        drag.current = { x: event.clientX, y: event.clientY };
        setView((v) => ({ ...v, x: v.x + dx, y: v.y + dy }));
      }}
      onPointerUp={() => {
        drag.current = null;
      }}
      onPointerCancel={() => {
        drag.current = null;
      }}
      onLostPointerCapture={() => {
        drag.current = null;
      }}
    >
      <div
        ref={world}
        className="flow-world"
        style={{
          transform: `translate(${view.x}px, ${view.y}px) scale(${view.scale})`,
        }}
      >
        {children}
      </div>
      <div className="canvas-hint">Scroll to zoom · Drag to pan</div>
      <div className="canvas-tools">
        <button
          type="button"
          aria-label="Zoom out"
          onClick={() => centerZoom(1 / 1.2)}
        >
          <Minus size={15} />
        </button>
        <output aria-label="Zoom level">{Math.round(view.scale * 100)}%</output>
        <button
          type="button"
          aria-label="Zoom in"
          onClick={() => centerZoom(1.2)}
        >
          <Plus size={15} />
        </button>
        <span />
        <button type="button" aria-label="Fit diagram" onClick={fit}>
          <Maximize2 size={15} />
        </button>
      </div>
    </div>
  );
}
