import { HashRouter, Route, Routes } from 'react-router-dom'
import { ThemeProvider } from './context/ThemeContext'
import { UserProvider } from './context/UserContext'
import { AppShell } from './components/shell/AppShell'
import { RoomList } from './components/RoomList'
import { RoomScreen } from './components/RoomScreen'
import { IntegrationsPage } from './components/integrations/IntegrationsPage'

export default function App() {
  return (
    <ThemeProvider>
      <HashRouter>
        <UserProvider>
          <Routes>
            <Route element={<AppShell />}>
              <Route path="/" element={<RoomList />} />
              <Route path="/rooms/:roomId" element={<RoomScreen />} />
              <Route path="/integrations" element={<IntegrationsPage />} />
              <Route path="*" element={<RoomList />} />
            </Route>
          </Routes>
        </UserProvider>
      </HashRouter>
    </ThemeProvider>
  )
}
