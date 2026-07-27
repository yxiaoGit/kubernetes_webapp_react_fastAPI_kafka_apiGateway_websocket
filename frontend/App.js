import React, { useState, useEffect, useRef } from 'react';

function App() {
  const [message, setMessage] = useState('');
  const [product, setProduct] = useState('');
  const [marketPrice, setMarketPrice] = useState(0);
  const [customBid, setCustomBid] = useState('');
  const [tradeStatus, setTradeStatus] = useState('');
  const socketRef = useRef(null);

  // REST Lookup Form Handler
  const handleLookup = async (e) => {
    e.preventDefault();
    setTradeStatus('');
    try {
      const response = await fetch('/api/lookup', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: message }),
      });
      const data = await response.json();
      setProduct(data.product);
      
      // Connect to the WebSocket when a valid product is loaded
      if (data.product && data.product !== "Unknown Product") {
        connectWebSocket();
      }
    } catch (err) {
      setProduct('Error contacting backend server.');
    }
  };

  // Real-time WebSocket Protocol Framework
  const connectWebSocket_NOT_Safari = () => {
    if (socketRef.current) socketRef.current.close();

    // Uses browser-native location rules to dynamically route past local port proxies
    const wsProto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
    const wsUrl = `${wsProto}${window.location.host}/ws/trading`;
    
    socketRef.current = new WebSocket(wsUrl);

    socketRef.current.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'PRICE_TICKER') {
        setMarketPrice(data.price);
      } else if (data.type === 'BID_RESULT') {
        setTradeStatus(data.status);
      }
    };
  };

  // Real-time WebSocket Protocol Framework (Safari Optimized)
  const connectWebSocket = () => {
    if (socketRef.current) {
      socketRef.current.close();
    }

    const isSecure = window.location.protocol === 'https:';
    const wsProto = isSecure ? 'wss://' : 'ws://';
    
    // Explicitly targets the dedicated /ws prefix route we established in app.yaml
    const wsUrl = `${wsProto}${window.location.host}/ws/trading`;
    
    console.log("Safari launching handshake to path target:", wsUrl);
    
    socketRef.current = new WebSocket(wsUrl);

    socketRef.current.onopen = () => {
      console.log("WebSocket tunnel completely established under Safari context!");
    };

    socketRef.current.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'PRICE_TICKER') {
        // Triggers the state hook to re-render the numbers on screen instantly
        setMarketPrice(data.price);
      } else if (data.type === 'BID_RESULT') {
        setTradeStatus(data.status);
      }
    };

    socketRef.current.onclose = (e) => {
      console.log("Safari socket connection disconnected. Re-syncing connection pool...", e.reason);
    };

    socketRef.current.onerror = (err) => {
      console.error("Safari internal socket error event:", err);
    };
  };
  // Send negotiation action block over Socket pipeline
  const submitBuyOffer = () => {
    if (socketRef.current && socketRef.current.readyState === WebSocket.OPEN) {
      setTradeStatus('Submitting bid to sellers for review...');
      socketRef.current.send(JSON.stringify({
        type: 'SUBMIT_BID',
        bid_price: customBid
      }));
    }
  };

  return (
    <div style={{ padding: '30px', fontFamily: 'Arial', maxWidth: '500px', margin: '0 auto' }}>
      <h2>Real-Time Negotiation Platform</h2>
      
      <form onSubmit={handleLookup}>
        <input 
          type="text" value={message} onChange={(e) => setMessage(e.target.value)} 
          placeholder="Type lookup (Try: 1234)" style={{ width: '100%', padding: '10px', marginBottom: '10px' }}
        />
        <button type="submit" style={{ width: '100%', padding: '10px', backgroundColor: '#007BFF', color: '#fff' }}>
          Fetch Product
        </button>
      </form>

      {product && product !== "Unknown Product" && (
        <div style={{ marginTop: '20px', padding: '15px', backgroundColor: '#EFFFF4', borderRadius: '5px' }}>
          <h3>Selected Item: {product}</h3>
          <h2 style={{ color: '#DC3545' }}>Live Price Feed: ${marketPrice} / sec</h2>
          
          <div style={{ marginTop: '15px' }}>
            <input 
              type="number" value={customBid} onChange={(e) => setCustomBid(e.target.value)} 
              placeholder="Enter your custom target bid price" style={{ width: '70%', padding: '8px', marginRight: '5px' }}
            />
            <button onClick={submitBuyOffer} style={{ padding: '8px', backgroundColor: '#28A745', color: '#white' }}>
              Place Bid
            </button>
          </div>
          
          {tradeStatus && (
            <div style={{ marginTop: '15px', padding: '10px', backgroundColor: '#FFF3CD', borderLeft: '4px solid #FFC107' }}>
              <strong>Consensus Verdict:</strong> {tradeStatus}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default App;
