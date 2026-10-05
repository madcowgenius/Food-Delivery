/**
 * FoodPulse — Web Dashboard Application Logic
 * Full mock data + live API dual-mode. Each tab maps to a real microservice.
 */

// ═══════════════════════════════════════════════════
// MOCK DATA STORE
// ═══════════════════════════════════════════════════

const DB = {
  customers: [
    { id: 'CUST-001', name: 'Helena Shikongo', email: 'helena.s@example.na', phone: '+264 81 245 8899', address: '44 Independence Ave, Windhoek' },
    { id: 'CUST-002', name: 'Petrus Nangolo', email: 'petrus.n@example.na', phone: '+264 81 445 1100', address: '18 Robert Mugabe Ave, Windhoek' },
    { id: 'CUST-003', name: 'Maria Shipanga', email: 'maria.s@example.na', phone: '+264 81 999 1122', address: '7 Sam Nujoma Drive, Klein Windhoek' },
    { id: 'CUST-004', name: 'David Alweendo', email: 'david.a@example.na', phone: '+264 81 990 4321', address: '25 Hosea Kutako Drive, Windhoek' },
  ],

  restaurants: [
    {
      id: 'REST-001', name: 'Namibian Grill & Braai House', cuisine: 'Traditional African Grill',
      address: 'Nelson Mandela Ave, Windhoek East', rating: 4.9, isOpen: true,
      menu: [
        { id: 'M-101', name: 'Grilled Kapana Meat Platter', price: 85.00, desc: 'Fresh Windhoek beef with authentic chili-salt rub' },
        { id: 'M-102', name: 'Traditional Oshithima & Chicken Stew', price: 65.00, desc: 'Pearl millet porridge with free-range chicken' },
        { id: 'M-103', name: 'Windhoek Pretzel & Herb Butter', price: 30.00, desc: 'Golden salted artisan bread' },
        { id: 'M-104', name: 'Kalahari Rooibos Iced Tea', price: 20.00, desc: 'Brewed rooibos with lemon and desert honey' },
      ]
    },
    {
      id: 'REST-002', name: 'Windhoek Pizza & Pasta Bar', cuisine: 'Italian Woodfired',
      address: 'Sam Nujoma Drive, Klein Windhoek', rating: 4.8, isOpen: true,
      menu: [
        { id: 'M-201', name: 'Kalahari Truffle & Game Pizza', price: 110.00, desc: 'Woodfired pizza with springbok carpaccio' },
        { id: 'M-202', name: 'Classic Margherita Basilico', price: 80.00, desc: 'San Marzano tomatoes, fresh mozzarella' },
        { id: 'M-203', name: 'Creamy Biltong Fettuccine', price: 95.00, desc: 'Handmade pasta with spiced beef biltong' },
        { id: 'M-204', name: 'Tiramisu Dolce', price: 45.00, desc: 'Traditional Italian coffee-soaked dessert' },
      ]
    },
    {
      id: 'REST-003', name: 'Kalahari Burger Café', cuisine: 'Craft Burgers & Shakes',
      address: 'Independence Ave, Windhoek Central', rating: 4.7, isOpen: true,
      menu: [
        { id: 'M-301', name: 'Oryx Steak Burger & Sweet Fries', price: 98.00, desc: 'Charcoal-grilled game patty with smoked cheddar' },
        { id: 'M-302', name: 'Double Kapana Slider Duo', price: 75.00, desc: 'Two brioche buns with caramelized onions' },
        { id: 'M-303', name: 'Salted Caramel Milkshake', price: 35.00, desc: 'Thick shake with Swakopmund mineral salt' },
      ]
    }
  ],

  drivers: [
    { id: 'DRV-101', name: 'Johannes Amukoto', vehicle: 'Yamaha Scooter (N 4521 W)', status: 'AVAILABLE', rating: 4.96, trips: 148, lat: -22.5609, lon: 17.0658 },
    { id: 'DRV-102', name: 'Festus Hamunyela', vehicle: 'Toyota Vitz (N 1104 W)', status: 'AVAILABLE', rating: 5.00, trips: 220, lat: -22.5530, lon: 17.0540 },
    { id: 'DRV-103', name: 'Ndapandula Shaanika', vehicle: 'Electric Scooter (N 3012 W)', status: 'AVAILABLE', rating: 4.92, trips: 84, lat: -22.5660, lon: 17.0810 },
    { id: 'DRV-104', name: 'Simaneka Haimbili', vehicle: 'Honda Bike (N 8892 W)', status: 'OFFLINE', rating: 4.85, trips: 67, lat: -22.5710, lon: 17.0720 },
  ],

  orders: [],
  payments: [],
  deliveries: [],
  notifications: [],
  events: [],

  // counters
  nextOrderNum: 1001,
  nextPayNum: 3001,
  nextDelNum: 5001,
  nextNotifNum: 7001,
  nextCustNum: 5,
};

// ═══════════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', () => {
  setupNavigation();
  setupCustomerForm();
  setupOrderForm();
  setupEventClear();
  setupNotifClear();

  // Seed some demo orders so the dashboard isn't empty
  seedDemoOrders();

  // Initial renders
  renderAll();
});

function seedDemoOrders() {
  // Order 1: Delivered (full lifecycle completed)
  placeOrderInternal('CUST-001', 'REST-001', ['M-101', 'M-102', 'M-104'], '44 Independence Ave, Windhoek', true);
  // advance through full lifecycle silently
  const o1 = DB.orders[0];
  processPaymentInternal(o1.id, true);
  advanceKitchenInternal(o1.id, 'PREPARING');
  advanceKitchenInternal(o1.id, 'READY');
  assignDriverInternal(o1.id);
  completeDeliveryInternal(DB.deliveries[0].id);

  // Order 2: Currently in PREPARING (partially through)
  placeOrderInternal('CUST-002', 'REST-002', ['M-201', 'M-204'], '18 Robert Mugabe Ave, Windhoek', true);
  const o2 = DB.orders[1];
  processPaymentInternal(o2.id, true);
  advanceKitchenInternal(o2.id, 'PREPARING');

  // Order 3: CONFIRMED waiting in kitchen
  placeOrderInternal('CUST-003', 'REST-003', ['M-301', 'M-303'], '7 Sam Nujoma Drive, Klein Windhoek', true);
  const o3 = DB.orders[2];
  processPaymentInternal(o3.id, true);

  // Order 4: Just placed, awaiting payment
  placeOrderInternal('CUST-004', 'REST-001', ['M-101', 'M-103'], '25 Hosea Kutako Drive, Windhoek', true);
}

// ═══════════════════════════════════════════════════
// NAVIGATION
// ═══════════════════════════════════════════════════

function setupNavigation() {
  document.querySelectorAll('.nav-link').forEach(link => {
    link.addEventListener('click', () => {
      const target = link.dataset.view;
      document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
      document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
      link.classList.add('active');
      document.getElementById('view-' + target).classList.add('active');
      renderAll();
    });
  });
}

// ═══════════════════════════════════════════════════
// RENDER ALL
// ═══════════════════════════════════════════════════

function renderAll() {
  renderDashboard();
  renderCustomers();
  renderRestaurants();
  renderOrders();
  renderPayments();
  renderKitchen();
  renderDeliveries();
  renderDrivers();
  renderNotifications();
  renderEvents();
  updateBadges();
}

function updateBadges() {
  const active = DB.orders.filter(o => o.status !== 'DELIVERED' && o.status !== 'CANCELLED').length;
  document.getElementById('orders-badge').textContent = active;

  const kitchenCount = DB.orders.filter(o => ['CONFIRMED', 'PREPARING', 'READY'].includes(o.status)).length;
  document.getElementById('kitchen-badge').textContent = kitchenCount;

  document.getElementById('notif-badge').textContent = DB.notifications.length;
}

// ═══════════════════════════════════════════════════
// DASHBOARD
// ═══════════════════════════════════════════════════

function renderDashboard() {
  document.getElementById('kpi-orders').textContent = DB.orders.length;
  document.getElementById('kpi-customers').textContent = DB.customers.length;

  const gmv = DB.orders.filter(o => o.status === 'DELIVERED').reduce((s, o) => s + o.total, 0);
  document.getElementById('kpi-revenue').textContent = 'N$ ' + gmv.toFixed(2);
  document.getElementById('kpi-commission').textContent = 'N$ ' + (gmv * 0.10).toFixed(2);

  const tbody = document.getElementById('dash-recent-orders');
  tbody.innerHTML = '';
  DB.orders.slice(-6).reverse().forEach(o => {
    tbody.innerHTML += `<tr>
      <td class="mono fw-700 text-white">#${o.id}</td>
      <td>${o.customerName}</td>
      <td class="fw-700 text-cyan">N$ ${o.total.toFixed(2)}</td>
      <td><span class="status status-${o.status.toLowerCase()}">${o.status}</span></td>
    </tr>`;
  });
}

// ═══════════════════════════════════════════════════
// CUSTOMERS
// ═══════════════════════════════════════════════════

function setupCustomerForm() {
  document.getElementById('form-add-customer').addEventListener('submit', (e) => {
    e.preventDefault();
    const name = document.getElementById('c-name').value.trim();
    const email = document.getElementById('c-email').value.trim();
    const phone = document.getElementById('c-phone').value.trim();
    const address = document.getElementById('c-address').value.trim();

    if (!name || !email || !phone) return toast('Please fill all required fields', 'warn');

    const id = 'CUST-' + String(DB.nextCustNum++).padStart(3, '0');
    DB.customers.push({ id, name, email, phone, address: address || 'Windhoek Central' });

    logEvent('customers.registered', `Customer ${name} registered with ID ${id}`);
    addNotification('EMAIL', email, `Welcome to FoodPulse, ${name}! Your account ${id} is ready.`);
    toast(`Customer "${name}" registered successfully!`, 'success');
    e.target.reset();
    renderAll();
  });
}

function renderCustomers() {
  const tbody = document.getElementById('tbl-customers');
  tbody.innerHTML = '';
  DB.customers.forEach(c => {
    tbody.innerHTML += `<tr>
      <td class="mono fw-700 text-white">${c.id}</td>
      <td class="fw-700">${c.name}</td>
      <td>${c.phone}</td>
      <td>${c.email}</td>
      <td><button class="btn btn-xs btn-ghost" onclick="deleteCustomer('${c.id}')">🗑️</button></td>
    </tr>`;
  });
}

function deleteCustomer(id) {
  DB.customers = DB.customers.filter(c => c.id !== id);
  toast('Customer removed', 'warn');
  renderAll();
}

// ═══════════════════════════════════════════════════
// RESTAURANTS
// ═══════════════════════════════════════════════════

function renderRestaurants() {
  const grid = document.getElementById('restaurants-grid');
  grid.innerHTML = '';
  DB.restaurants.forEach(r => {
    let menuHtml = r.menu.map(m =>
      `<div class="menu-row">
        <div>
          <div class="menu-row-name">${m.name}</div>
          <div class="menu-row-desc">${m.desc}</div>
        </div>
        <div class="menu-row-price">N$ ${m.price.toFixed(2)}</div>
      </div>`
    ).join('');

    grid.innerHTML += `
      <div class="rest-card">
        <div class="rest-card-head">
          <div>
            <div class="rest-name">${r.name}</div>
            <div class="rest-cuisine">${r.cuisine} • ${r.address}</div>
          </div>
          <div class="rest-rating">⭐ ${r.rating}</div>
        </div>
        <div class="rest-menu-list">${menuHtml}</div>
      </div>`;
  });
}

// ═══════════════════════════════════════════════════
// ORDERS
// ═══════════════════════════════════════════════════

function setupOrderForm() {
  const custSelect = document.getElementById('o-customer');
  const restSelect = document.getElementById('o-restaurant');
  const menuDiv = document.getElementById('o-menu-items');

  // Populate customer dropdown
  function populateCustomerDropdown() {
    custSelect.innerHTML = '<option value="">-- Select Customer --</option>';
    DB.customers.forEach(c => {
      custSelect.innerHTML += `<option value="${c.id}">${c.name} (${c.phone})</option>`;
    });
  }

  // Populate restaurant dropdown
  restSelect.innerHTML = '<option value="">-- Select Restaurant --</option>';
  DB.restaurants.forEach(r => {
    restSelect.innerHTML += `<option value="${r.id}">${r.name}</option>`;
  });

  // When customer changes, fill address
  custSelect.addEventListener('change', () => {
    const c = DB.customers.find(x => x.id === custSelect.value);
    if (c) document.getElementById('o-address').value = c.address || '';
  });

  // When restaurant changes, show menu checkboxes
  restSelect.addEventListener('change', () => {
    const rest = DB.restaurants.find(r => r.id === restSelect.value);
    menuDiv.innerHTML = '';
    if (!rest) return;
    rest.menu.forEach(m => {
      menuDiv.innerHTML += `
        <label class="menu-check-item">
          <input type="checkbox" value="${m.id}" data-price="${m.price}" class="menu-cb">
          <div class="menu-check-info">
            <div class="menu-check-name">${m.name}</div>
          </div>
          <div class="menu-check-price">N$ ${m.price.toFixed(2)}</div>
        </label>`;
    });
    // Bind checkboxes to recalc total
    menuDiv.querySelectorAll('.menu-cb').forEach(cb => {
      cb.addEventListener('change', recalcTotal);
    });
    recalcTotal();
  });

  function recalcTotal() {
    let total = 0;
    menuDiv.querySelectorAll('.menu-cb:checked').forEach(cb => {
      total += parseFloat(cb.dataset.price);
    });
    document.getElementById('o-total').textContent = 'N$ ' + total.toFixed(2);
  }

  // Submit order
  document.getElementById('form-place-order').addEventListener('submit', (e) => {
    e.preventDefault();
    const custId = custSelect.value;
    const restId = restSelect.value;
    const address = document.getElementById('o-address').value.trim();

    if (!custId) return toast('Please select a customer', 'warn');
    if (!restId) return toast('Please select a restaurant', 'warn');

    const selectedItems = [];
    menuDiv.querySelectorAll('.menu-cb:checked').forEach(cb => selectedItems.push(cb.value));
    if (selectedItems.length === 0) return toast('Please select at least one menu item', 'warn');

    placeOrderInternal(custId, restId, selectedItems, address || 'Windhoek Central', false);
    toast('Order placed! Kafka event: orders.created', 'success');
    renderAll();

    // Reset form
    restSelect.value = '';
    menuDiv.innerHTML = '';
    document.getElementById('o-total').textContent = 'N$ 0.00';
  });

  // Initial populate
  populateCustomerDropdown();

  // Re-populate when customer list changes (observe via MutationObserver alternative: just call on render)
  const origRender = renderOrders;
  // We hook into renderAll via a simpler approach
  window._refreshOrderForm = populateCustomerDropdown;
}

function placeOrderInternal(custId, restId, itemIds, address, silent) {
  const customer = DB.customers.find(c => c.id === custId);
  const restaurant = DB.restaurants.find(r => r.id === restId);
  if (!customer || !restaurant) return;

  const items = restaurant.menu.filter(m => itemIds.includes(m.id));
  const total = items.reduce((s, i) => s + i.price, 0);

  const orderId = 'ORD-' + (DB.nextOrderNum++);
  const order = {
    id: orderId,
    customerId: custId,
    customerName: customer.name,
    customerPhone: customer.phone,
    restaurantId: restId,
    restaurantName: restaurant.name,
    items: items.map(i => ({ id: i.id, name: i.name, price: i.price, qty: 1 })),
    total,
    address,
    status: 'CREATED',
    paymentMethod: 'CREDIT_CARD',
    createdAt: new Date(),
  };

  DB.orders.push(order);

  if (!silent) {
    logEvent('orders.created', `Order #${orderId} placed by ${customer.name} at ${restaurant.name}. Total: N$ ${total.toFixed(2)}`);
    addNotification('SMS', customer.phone, `Order #${orderId} received! Total: N$ ${total.toFixed(2)}. We are notifying the kitchen.`);
    addNotification('PUSH', customer.name, `Your order at ${restaurant.name} is being processed.`);
  }
}

function renderOrders() {
  if (window._refreshOrderForm) window._refreshOrderForm();

  const tbody = document.getElementById('tbl-orders');
  tbody.innerHTML = '';
  DB.orders.slice().reverse().forEach(o => {
    let actions = '';
    if (o.status === 'CREATED') {
      actions = `<button class="btn btn-xs btn-warning" onclick="navigateTo('payments')">Pay →</button>`;
    } else if (o.status === 'CONFIRMED') {
      actions = `<button class="btn btn-xs btn-secondary" onclick="navigateTo('kitchen')">Kitchen →</button>`;
    } else if (o.status === 'READY') {
      actions = `<button class="btn btn-xs btn-secondary" onclick="navigateTo('deliveries')">Deliver →</button>`;
    }

    tbody.innerHTML += `<tr>
      <td class="mono fw-700 text-white">#${o.id}</td>
      <td>${o.customerName}</td>
      <td>${o.restaurantName}</td>
      <td class="fw-700 text-cyan">N$ ${o.total.toFixed(2)}</td>
      <td><span class="status status-${o.status.toLowerCase()}">${o.status}</span></td>
      <td>${actions}</td>
    </tr>`;
  });
}

function navigateTo(viewName) {
  document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.getElementById('nav-' + viewName).classList.add('active');
  document.getElementById('view-' + viewName).classList.add('active');
  renderAll();
}

// ═══════════════════════════════════════════════════
// PAYMENTS
// ═══════════════════════════════════════════════════

function renderPayments() {
  // Pending payments = orders in CREATED status
  const pending = DB.orders.filter(o => o.status === 'CREATED');
  const tbody = document.getElementById('tbl-pending-payments');
  tbody.innerHTML = '';
  pending.forEach(o => {
    tbody.innerHTML += `<tr>
      <td class="mono fw-700 text-white">#${o.id}</td>
      <td>${o.customerName}</td>
      <td class="fw-700 text-cyan">N$ ${o.total.toFixed(2)}</td>
      <td>Credit Card</td>
      <td>
        <button class="btn btn-xs btn-success" onclick="processPayment('${o.id}')">✓ Process Payment</button>
        <button class="btn btn-xs btn-danger" onclick="failPayment('${o.id}')" style="margin-left:4px;">✗ Simulate Fail</button>
      </td>
    </tr>`;
  });
  if (pending.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="text-muted" style="text-align:center;padding:24px;">No pending payments</td></tr>';
  }

  // Payment history
  const histTbody = document.getElementById('tbl-payment-history');
  histTbody.innerHTML = '';
  DB.payments.slice().reverse().forEach(p => {
    histTbody.innerHTML += `<tr>
      <td class="mono fw-700 text-white">${p.id}</td>
      <td class="mono">${p.orderId}</td>
      <td class="fw-700 text-cyan">N$ ${p.amount.toFixed(2)}</td>
      <td>${p.method}</td>
      <td><span class="status status-${p.status.toLowerCase()}">${p.status}</span></td>
      <td class="mono text-muted">${p.time}</td>
    </tr>`;
  });
}

function processPayment(orderId) {
  processPaymentInternal(orderId, true);
  toast(`Payment for #${orderId} processed. Kafka: payments.completed`, 'success');
  renderAll();
}

function failPayment(orderId) {
  processPaymentInternal(orderId, false);
  toast(`Payment for #${orderId} failed! Kafka: payments.failed`, 'error');
  renderAll();
}

function processPaymentInternal(orderId, success) {
  const order = DB.orders.find(o => o.id === orderId);
  if (!order || order.status !== 'CREATED') return;

  const payId = 'PAY-' + (DB.nextPayNum++);
  const payment = {
    id: payId,
    orderId,
    amount: order.total,
    method: 'CREDIT_CARD',
    status: success ? 'SUCCESS' : 'FAILED',
    time: new Date().toLocaleTimeString('en-GB'),
  };
  DB.payments.push(payment);

  if (success) {
    order.status = 'CONFIRMED';
    logEvent('payments.completed', `Payment ${payId} for order #${orderId}: N$ ${order.total.toFixed(2)} charged successfully.`);
    logEvent('orders.confirmed', `Order #${orderId} confirmed. State: CREATED → CONFIRMED.`);
    addNotification('SMS', order.customerPhone, `Payment confirmed for order #${orderId}. Kitchen has been notified!`);
  } else {
    order.status = 'CANCELLED';
    logEvent('payments.failed', `Payment FAILED for order #${orderId}. Order cancelled.`);
    logEvent('orders.cancelled', `Order #${orderId} cancelled due to payment failure.`);
    addNotification('SMS', order.customerPhone, `Payment failed for order #${orderId}. Please try again.`);
  }
}

// ═══════════════════════════════════════════════════
// KITCHEN DISPLAY SYSTEM (KDS)
// ═══════════════════════════════════════════════════

function renderKitchen() {
  const confirmed = DB.orders.filter(o => o.status === 'CONFIRMED');
  const preparing = DB.orders.filter(o => o.status === 'PREPARING');
  const ready = DB.orders.filter(o => o.status === 'READY');

  document.getElementById('kds-count-confirmed').textContent = confirmed.length;
  document.getElementById('kds-count-preparing').textContent = preparing.length;
  document.getElementById('kds-count-ready').textContent = ready.length;

  renderKdsColumn('kds-confirmed', confirmed, 'PREPARING', '🍳 Start Cooking', 'btn-warning');
  renderKdsColumn('kds-preparing', preparing, 'READY', '📦 Mark Ready', 'btn-success');
  renderKdsColumn('kds-ready', ready, null, null, null);
}

function renderKdsColumn(containerId, orders, nextStatus, btnText, btnClass) {
  const el = document.getElementById(containerId);
  el.innerHTML = '';
  orders.forEach(o => {
    const itemsList = o.items.map(i => `${i.name} × ${i.qty}`).join(', ');
    const elapsed = Math.floor((Date.now() - new Date(o.createdAt).getTime()) / 60000);

    let actionBtn = '';
    if (nextStatus) {
      actionBtn = `<button class="btn btn-xs ${btnClass}" onclick="advanceKitchen('${o.id}', '${nextStatus}')">${btnText}</button>`;
    } else {
      actionBtn = `<span class="status status-ready">Awaiting Driver</span>`;
    }

    el.innerHTML += `
      <div class="kds-ticket">
        <div class="kds-ticket-head">
          <span class="kds-ticket-id">#${o.id}</span>
          <span class="kds-ticket-time">⏱️ ${elapsed}m ago</span>
        </div>
        <div class="kds-ticket-customer">👤 ${o.customerName}</div>
        <div class="kds-ticket-items">${itemsList}</div>
        <div class="kds-ticket-foot">
          <span class="kds-ticket-total">N$ ${o.total.toFixed(2)}</span>
          ${actionBtn}
        </div>
      </div>`;
  });

  if (orders.length === 0) {
    el.innerHTML = '<div class="text-muted" style="text-align:center;padding:40px 0;font-size:0.82rem;">No orders in this stage</div>';
  }
}

function advanceKitchen(orderId, newStatus) {
  advanceKitchenInternal(orderId, newStatus);
  const label = newStatus === 'PREPARING' ? 'orders.preparing' : 'orders.ready';
  toast(`Order #${orderId} → ${newStatus}. Kafka: ${label}`, 'success');
  renderAll();
}

function advanceKitchenInternal(orderId, newStatus) {
  const order = DB.orders.find(o => o.id === orderId);
  if (!order) return;
  order.status = newStatus;

  if (newStatus === 'PREPARING') {
    logEvent('orders.preparing', `Kitchen started cooking order #${orderId}. State: CONFIRMED → PREPARING.`);
    addNotification('PUSH', order.customerName, `Chef started preparing your order #${orderId}!`);
  } else if (newStatus === 'READY') {
    logEvent('orders.ready', `Order #${orderId} packaged and ready for pickup. State: PREPARING → READY.`);
    addNotification('SMS', order.customerPhone, `Your order #${orderId} is ready! Assigning a driver now.`);
  }
}

// ═══════════════════════════════════════════════════
// DELIVERIES
// ═══════════════════════════════════════════════════

function renderDeliveries() {
  // Available drivers count
  const avail = DB.drivers.filter(d => d.status === 'AVAILABLE').length;
  const busy = DB.drivers.filter(d => d.status === 'BUSY').length;
  const completed = DB.deliveries.filter(d => d.status === 'DELIVERED').length;
  document.getElementById('del-avail').textContent = avail;
  document.getElementById('del-busy').textContent = busy;
  document.getElementById('del-completed').textContent = completed;

  // Ready for dispatch
  const readyOrders = DB.orders.filter(o => o.status === 'READY');
  const tbody1 = document.getElementById('tbl-ready-dispatch');
  tbody1.innerHTML = '';
  readyOrders.forEach(o => {
    tbody1.innerHTML += `<tr>
      <td class="mono fw-700 text-white">#${o.id}</td>
      <td>${o.restaurantName}</td>
      <td>${o.address}</td>
      <td><button class="btn btn-xs btn-primary" onclick="assignDriver('${o.id}')" ${avail === 0 ? 'disabled' : ''}>
        🛵 Assign Driver
      </button></td>
    </tr>`;
  });
  if (readyOrders.length === 0) {
    tbody1.innerHTML = '<tr><td colspan="4" class="text-muted" style="text-align:center;padding:24px;">No orders ready for dispatch</td></tr>';
  }

  // Active deliveries
  const tbody2 = document.getElementById('tbl-deliveries');
  tbody2.innerHTML = '';
  DB.deliveries.slice().reverse().forEach(d => {
    const driver = DB.drivers.find(dr => dr.id === d.driverId);
    let action = '';
    if (d.status === 'PICKED_UP') {
      action = `<button class="btn btn-xs btn-success" onclick="completeDelivery('${d.id}')">✓ Mark Delivered</button>`;
    } else if (d.status === 'ASSIGNED') {
      action = `<button class="btn btn-xs btn-secondary" onclick="pickupDelivery('${d.id}')">📦 Confirm Pickup</button>`;
    }

    tbody2.innerHTML += `<tr>
      <td class="mono fw-700 text-white">${d.id}</td>
      <td>${driver ? driver.name : '—'}</td>
      <td class="mono">#${d.orderId}</td>
      <td><span class="status status-${d.status === 'ASSIGNED' ? 'preparing' : d.status === 'PICKED_UP' ? 'out_for_delivery' : 'delivered'}">${d.status}</span></td>
      <td>${action}</td>
    </tr>`;
  });
}

function assignDriver(orderId) {
  assignDriverInternal(orderId);
  toast(`Driver assigned to order #${orderId}! Kafka: delivery.assigned`, 'success');
  renderAll();
}

function assignDriverInternal(orderId) {
  const order = DB.orders.find(o => o.id === orderId);
  if (!order || order.status !== 'READY') return;

  const driver = DB.drivers.find(d => d.status === 'AVAILABLE');
  if (!driver) return;

  driver.status = 'BUSY';
  order.status = 'OUT_FOR_DELIVERY';

  const delId = 'DEL-' + (DB.nextDelNum++);
  DB.deliveries.push({
    id: delId,
    orderId,
    driverId: driver.id,
    status: 'ASSIGNED',
    distanceKm: (2 + Math.random() * 4).toFixed(1),
    etaMins: Math.floor(8 + Math.random() * 12),
    createdAt: new Date(),
  });

  logEvent('delivery.assigned', `Driver ${driver.name} assigned to order #${orderId}. Distance: ${DB.deliveries.at(-1).distanceKm} km, ETA: ${DB.deliveries.at(-1).etaMins} mins.`);
  addNotification('SMS', order.customerPhone, `Driver ${driver.name} (${driver.vehicle}) is on the way! ETA: ~${DB.deliveries.at(-1).etaMins} mins.`);
}

function pickupDelivery(delId) {
  const del = DB.deliveries.find(d => d.id === delId);
  if (!del) return;
  del.status = 'PICKED_UP';
  const order = DB.orders.find(o => o.id === del.orderId);
  const driver = DB.drivers.find(d => d.id === del.driverId);

  logEvent('delivery.picked_up', `Driver ${driver ? driver.name : '?'} picked up food for order #${del.orderId}.`);
  addNotification('PUSH', order ? order.customerName : '?', `Your food has been picked up and is on the way!`);
  toast(`Pickup confirmed for ${delId}. Kafka: delivery.picked_up`, 'success');
  renderAll();
}

function completeDelivery(delId) {
  completeDeliveryInternal(delId);
  toast(`Delivery ${delId} completed! Kafka: delivery.completed`, 'success');
  renderAll();
}

function completeDeliveryInternal(delId) {
  const del = DB.deliveries.find(d => d.id === delId);
  if (!del) return;
  del.status = 'DELIVERED';

  const order = DB.orders.find(o => o.id === del.orderId);
  if (order) order.status = 'DELIVERED';

  const driver = DB.drivers.find(d => d.id === del.driverId);
  if (driver) {
    driver.status = 'AVAILABLE';
    driver.trips++;
  }

  logEvent('delivery.completed', `Order #${del.orderId} delivered to customer. Driver ${driver ? driver.name : '?'} is now AVAILABLE.`);
  if (order) {
    addNotification('SMS', order.customerPhone, `Your food has arrived! 🎉 Order #${del.orderId} delivered. Enjoy your meal!`);
  }
}

// ═══════════════════════════════════════════════════
// DRIVERS
// ═══════════════════════════════════════════════════

function renderDrivers() {
  const tbody = document.getElementById('tbl-drivers');
  tbody.innerHTML = '';
  DB.drivers.forEach(d => {
    tbody.innerHTML += `<tr>
      <td class="mono fw-700 text-white">${d.id}</td>
      <td class="fw-700">${d.name}</td>
      <td>${d.vehicle}</td>
      <td><span class="status status-${d.status.toLowerCase()}">${d.status}</span></td>
      <td class="text-amber fw-700">⭐ ${d.rating.toFixed(2)}</td>
      <td class="fw-700">${d.trips}</td>
      <td class="mono text-muted" style="font-size:0.75rem;">${d.lat.toFixed(4)}, ${d.lon.toFixed(4)}</td>
    </tr>`;
  });
}

// ═══════════════════════════════════════════════════
// NOTIFICATIONS
// ═══════════════════════════════════════════════════

function addNotification(channel, recipient, message) {
  DB.notifications.unshift({
    id: 'NOTIF-' + (DB.nextNotifNum++),
    channel,
    recipient,
    message,
    time: new Date().toLocaleTimeString('en-GB'),
  });
}

function renderNotifications() {
  const tbody = document.getElementById('tbl-notifications');
  tbody.innerHTML = '';
  DB.notifications.slice(0, 40).forEach(n => {
    const icon = n.channel === 'SMS' ? '📱' : n.channel === 'EMAIL' ? '📧' : '🔔';
    tbody.innerHTML += `<tr>
      <td class="mono text-muted">${n.time}</td>
      <td>${icon} ${n.channel}</td>
      <td class="fw-700">${n.recipient}</td>
      <td>${n.message}</td>
    </tr>`;
  });
}

function setupNotifClear() {
  document.getElementById('btn-clear-notifs').addEventListener('click', () => {
    DB.notifications = [];
    renderNotifications();
    updateBadges();
  });
}

// ═══════════════════════════════════════════════════
// KAFKA EVENT LOG
// ═══════════════════════════════════════════════════

function logEvent(topic, message) {
  DB.events.unshift({
    topic,
    message,
    time: new Date().toLocaleTimeString('en-GB'),
  });
}

function renderEvents() {
  const el = document.getElementById('event-log');
  el.innerHTML = '';
  DB.events.slice(0, 30).forEach(e => {
    let dotColor = 'var(--indigo)';
    if (e.topic.startsWith('payments')) dotColor = 'var(--emerald)';
    else if (e.topic.startsWith('delivery')) dotColor = 'var(--cyan)';
    else if (e.topic.startsWith('notifications')) dotColor = 'var(--amber)';
    else if (e.topic.includes('cancelled') || e.topic.includes('failed')) dotColor = 'var(--rose)';

    el.innerHTML += `
      <div class="event-item">
        <span class="event-dot" style="background:${dotColor};box-shadow:0 0 6px ${dotColor};"></span>
        <div class="event-body">
          <div class="event-topic">${e.topic}</div>
          <div class="event-msg">${e.message}</div>
        </div>
        <span class="event-time">${e.time}</span>
      </div>`;
  });
}

function setupEventClear() {
  document.getElementById('btn-clear-events').addEventListener('click', () => {
    DB.events = [];
    renderEvents();
  });
}

// ═══════════════════════════════════════════════════
// TOAST
// ═══════════════════════════════════════════════════

function toast(message, type = 'info') {
  const rack = document.getElementById('toast-rack');
  const el = document.createElement('div');
  el.className = 'toast ' + type;
  const icon = type === 'success' ? '✅' : type === 'warn' ? '⚠️' : type === 'error' ? '❌' : 'ℹ️';
  el.innerHTML = `<span>${icon}</span><span>${message}</span>`;
  rack.appendChild(el);
  setTimeout(() => {
    el.style.opacity = '0';
    el.style.transform = 'translateX(20px)';
    el.style.transition = 'all 0.2s ease';
    setTimeout(() => el.remove(), 200);
  }, 3500);
}
