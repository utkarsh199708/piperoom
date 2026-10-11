import { HashRouter, Route, Routes } from 'react-router-dom'
import { UserProvider } from './context/UserContext'
import { RoomList } from './components/RoomList'
import { RoomScreen } from './components/RoomScreen'

export default function App() {
  return (
    <HashRouter>
      <UserProvider>
        <Routes>
          <Route path="/" element={<RoomList />} />
          <Route path="/rooms/:roomId" element={<RoomScreen />} />
          <Route path="*" element={<RoomList />} />
        </Routes>
      </UserProvider>
    </HashRouter>
  )
}
