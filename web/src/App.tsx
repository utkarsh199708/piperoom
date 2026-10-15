import { HashRouter, Route, Routes, useParams } from 'react-router-dom'
import { ThemeProvider } from './context/ThemeContext'
import { UserProvider } from './context/UserContext'
import { AppShell } from './components/shell/AppShell'
import { RoomList } from './components/RoomList'
import { RoomScreen } from './components/RoomScreen'

// Keyed by the room id, so every per-room useState (dialogs, tabs, filters) resets on a jump to another room.
export function RoomScreenRoute() {
  const { roomId } = useParams()
  return <RoomScreen key={roomId} />
}

export default function App() {
  return (
    <ThemeProvider>
      <HashRouter>
        <UserProvider>
          <Routes>
            <Route element={<AppShell />}>
              <Route path="/" element={<RoomList />} />
              <Route path="/rooms/:roomId" element={<RoomScreenRoute />} />
              <Route path="*" element={<RoomList />} />
            </Route>
          </Routes>
        </UserProvider>
      </HashRouter>
    </ThemeProvider>
  )
}
