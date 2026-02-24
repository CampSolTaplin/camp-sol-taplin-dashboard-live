/**
 * Camp Sol Taplin - Push Notification Manager
 * Handles permission request, subscription, and unsubscription.
 */
var CampPush = (function() {

  function urlBase64ToUint8Array(base64String) {
    var padding = '='.repeat((4 - base64String.length % 4) % 4);
    var base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
    var rawData = atob(base64);
    var outputArray = new Uint8Array(rawData.length);
    for (var i = 0; i < rawData.length; i++) {
      outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
  }

  function isSupported() {
    return 'serviceWorker' in navigator && 'PushManager' in window && 'Notification' in window;
  }

  function getPermissionState() {
    if (!isSupported()) return 'unsupported';
    return Notification.permission; // 'default', 'granted', 'denied'
  }

  async function subscribe() {
    if (!isSupported()) return { success: false, reason: 'unsupported' };

    // Request permission
    var permission = await Notification.requestPermission();
    if (permission !== 'granted') {
      return { success: false, reason: 'denied' };
    }

    try {
      // Get VAPID public key
      var keyResp = await fetch('/api/push/vapid-key');
      if (!keyResp.ok) return { success: false, reason: 'no-vapid-key' };
      var keyData = await keyResp.json();

      // Subscribe via service worker
      var registration = await navigator.serviceWorker.ready;
      var subscription = await registration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(keyData.publicKey)
      });

      // Send subscription to server
      var subJson = subscription.toJSON();
      var resp = await fetch('/api/push/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          endpoint: subJson.endpoint,
          keys: subJson.keys
        })
      });

      if (!resp.ok) return { success: false, reason: 'server-error' };
      return { success: true };
    } catch (e) {
      console.error('Push subscribe error:', e);
      return { success: false, reason: e.message };
    }
  }

  async function unsubscribe() {
    try {
      var registration = await navigator.serviceWorker.ready;
      var subscription = await registration.pushManager.getSubscription();

      if (subscription) {
        var endpoint = subscription.endpoint;
        await subscription.unsubscribe();
        await fetch('/api/push/unsubscribe', {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ endpoint: endpoint })
        });
      }
      return { success: true };
    } catch (e) {
      console.error('Push unsubscribe error:', e);
      return { success: false, reason: e.message };
    }
  }

  async function isSubscribed() {
    if (!isSupported()) return false;
    try {
      var registration = await navigator.serviceWorker.ready;
      var subscription = await registration.pushManager.getSubscription();
      return subscription !== null;
    } catch (e) {
      return false;
    }
  }

  return {
    isSupported: isSupported,
    getPermissionState: getPermissionState,
    subscribe: subscribe,
    unsubscribe: unsubscribe,
    isSubscribed: isSubscribed
  };
})();
