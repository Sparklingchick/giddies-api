/**
 * GIDDIES EXPRESS — AUTH GUARD
 * Include this script at the TOP of every protected HTML page.
 * It BLOCKS the page from rendering until the server confirms the session.
 * Typing /admin.html directly will redirect to login if no valid token.
 */
(function() {
  // Hide everything immediately
  document.documentElement.style.visibility = 'hidden';

  var API = window.GX_API_URL || 'https://your-backend.railway.app';
  var PAGE_ROLE = window.GX_REQUIRED_ROLE || 'employee';
  var token = sessionStorage.getItem('gx_token');

  if (!token) {
    window.location.replace('/giddyexpress-login.html');
    return;
  }

  // Verify with server — this is the critical check
  fetch(API + '/api/auth/verify-role?required_role=' + PAGE_ROLE, {
    method: 'GET',
    headers: {
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json'
    }
  })
  .then(function(res) {
    if (res.ok) {
      return res.json();
    }
    return res.json().then(function(err) {
      throw new Error(err.detail || 'Unauthorized');
    });
  })
  .then(function(data) {
    // Restore visibility and boot the app
    document.documentElement.style.visibility = '';
    if (window.GX_BOOT) window.GX_BOOT(data.user);
  })
  .catch(function(err) {
    sessionStorage.removeItem('gx_token');
    sessionStorage.removeItem('gx_user');
    window.location.replace(
      '/giddyexpress-login.html?reason=' + 
      encodeURIComponent(err.message || 'Session expired')
    );
  });
})();
