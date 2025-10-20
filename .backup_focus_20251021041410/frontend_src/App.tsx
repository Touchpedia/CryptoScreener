import Controls from "./components/Controls";

export default function App() {
  return (
    <div style={{maxWidth:1100, margin:"24px auto", padding:16, fontFamily:"system-ui"}}>
      <h2>Crypto Screener — Controls</h2>
      <h3>Ingestion Controls</h3>
      <Controls />
      <hr style={{margin:"16px 0"}}/>
      <p>If other sections (Latest, Coverage) exist in your project, they will render below.</p>
    </div>
  );
}
