# Gesture-Controlled-Code-Editor
Control your code editor using hand gestures — because keyboards shouldn’t be the only interface. Modern IDEs are powerful, but interaction is still limited to keyboards and mice. This project introduces a gesture-based control layer for code editors, allowing developers to perform common IDE actions using hand gestures detected via a camera.

A gesture-based interaction system that allows developers to control a code editor using hand gestures detected through a camera.
The project is built as a modular, production-oriented system, not a demo, combining computer vision, backend services, and editor integration.

 Project Objective
To explore gesture control as a first-class interface for developer tools, enabling hands-free interaction with a code editor while maintaining low latency, accuracy, and extensibility.


 Core Capabilities
Real-time hand detection and tracking
Gesture recognition engine
Gesture → editor command mapping
VS Code integration via extension
Config-driven gesture bindings
Low-latency WebSocket communication
Clean separation of concerns across modules


 Product Architecture
Camera Feed
   ↓
Vision Module (Hand Tracking)
   ↓
Gesture Detection & Classification
   ↓
Gesture Engine (Mapping + State)
   ↓
Backend Server (WebSocket)
   ↓
Editor Extension (VS Code)
   ↓
IDE Command Execution


 
 Tech Stack
Core Language
Python – vision processing, gesture logic, backend
Vision & Gesture Detection
OpenCV
MediaPipe Hands
NumPy
Backend & Communication
FastAPI / Flask
WebSockets
JSON / YAML
Editor Integration
TypeScript
VS Code Extension API
Node.js
