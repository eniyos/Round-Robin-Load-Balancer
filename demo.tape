# VHS tape for recording L4 Load Balancer demo GIF
# https://github.com/charmbracelet/vhs

Output assets/demo.gif
Set FontSize 14
Set Width 900
Set Height 500
Set Theme "Catppuccin Mocha"
Set TypingSpeed 40ms
Set Padding 15

Type "# Start 3 mock HTTP backends"
Enter
Sleep 500ms

Type "python3 -m http.server 9001 --directory /tmp/lb_b1 &"
Enter
Sleep 300ms
Type "python3 -m http.server 9002 --directory /tmp/lb_b2 &"
Enter
Sleep 300ms
Type "python3 -m http.server 9003 --directory /tmp/lb_b3 &"
Enter
Sleep 1s

Type "# Start the load balancer"
Enter
Sleep 500ms
Type "python3 balancer.py &"
Enter
Sleep 2s

Type "# Round-robin: 6 requests rotate across 3 backends"
Enter
Sleep 500ms

Type "for i in 1 2 3 4 5 6; do curl -s http://127.0.0.1:8080/; done"
Enter
Sleep 3s

Type "# Kill backend 2 to trigger failover"
Enter
Sleep 500ms
Type "kill %2"
Enter
Sleep 4s

Type "# Requests now skip dead backend"
Enter
Sleep 500ms
Type "for i in 1 2 3 4; do curl -s http://127.0.0.1:8080/; done"
Enter
Sleep 3s

Type "# Revive backend 2"
Enter
Sleep 500ms
Type "python3 -m http.server 9002 --directory /tmp/lb_b2 &"
Enter
Sleep 4s

Type "# Backend 2 back in rotation"
Enter
Sleep 500ms
Type "for i in 1 2 3 4 5 6; do curl -s http://127.0.0.1:8080/; done"
Enter
Sleep 3s

Type "# Cleanup"
Enter
Type "kill %1 %3 %4 2>/dev/null; echo 'Done.'"
Enter
Sleep 1s
