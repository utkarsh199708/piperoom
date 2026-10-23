import { HashRouter, Route, Routes, useParams } from 'react-router-dom'
import { ThemeProvider } from './context/ThemeContext'
import { UserProvider } from './context/UserContext'
import { AppShell } from './components/shell/AppShell'
import { RoomList } from './components/RoomList'
import { RoomScreen } from './components/RoomScreen'
import { ShadowMetricsPage } from './components/shadow/ShadowMetricsPage'
import { SettingsPage } from './components/settings/SettingsPage'
import { FleetScreen } from './components/fleet/FleetScreen'
import { PipelineLanePage } from './components/fleet/PipelineLanePage'
import { SetupPage } from './components/setup/SetupPage'

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
              {/* The bare app URL lands on /fleet (main.tsx, landingHash); "/" stays the room list, so every
                  existing link (#/, #/?pipeline=..., #/rooms/...) opens what it always did. */}
              <Route path="/" element={<RoomList />} />
              <Route path="/fleet" element={<FleetScreen />} />
              {/* U1-12a: New pipeline → Set up opens a setup room. */}
              <Route path="/setup" element={<SetupPage />} />
              <Route path="/pipelines/:pipelineId" element={<PipelineLanePage />} />
              <Route path="/rooms/:roomId" element={<RoomScreenRoute />} />
              <Route path="/shadow" element={<ShadowMetricsPage />} />
              <Route path="/settings" element={<SettingsPage />} />
              <Route path="*" element={<RoomList />} />
            </Route>
          </Routes>
        </UserProvider>
      </HashRouter>
    </ThemeProvider>
  )
}
