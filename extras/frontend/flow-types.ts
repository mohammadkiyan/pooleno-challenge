export type NodeId =
  | "client"
  | "api"
  | "database"
  | "outbox"
  | "kafka"
  | "matching"
  | "worker"
  | "external";
export type PhaseId =
  | "api-validate"
  | "api-key"
  | "api-save"
  | "api-reply"
  | "match-guard"
  | "match-price"
  | "match-fill"
  | "match-save"
  | "match-book"
  | "match-restore"
  | "match-replay"
  | "publish-read"
  | "publish-send"
  | "publish-mark"
  | "publish-retry"
  | "notify-inbox"
  | "notify-send"
  | "notify-retry"
  | "notify-response"
  | "notify-complete";
export interface BookOrder {
  id: string;
  side: "BUY" | "SELL";
  price: string;
  quantity: string;
}
export interface Trade {
  taker?: string;
  maker: string;
  price: string;
  quantity: string;
}
export interface Frame {
  title: string;
  detail: string;
  active: NodeId[];
  blocked: NodeId[];
  parallel: NodeId[];
  book: BookOrder[];
  bookState?: "available" | "discarded";
  trades: Trade[];
  facts: [string, string][];
  request: string;
  response: string;
  requestLabel: string;
  decision: string;
  source: string;
  phase: PhaseId | null;
  transfers: [NodeId, NodeId][];
}
