/*
 * Copyright 2019 http4s.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.http4s.ember.core.h2

import cats.effect._
import cats.syntax.all._
import com.comcast.ip4s._
import fs2.Chunk
import fs2.Stream
import fs2.io.net.Network
import fs2.io.net.Socket
import org.http4s._
import org.http4s.h2.H2Keys
import scodec.bits.ByteVector

/** Fake h2 (prior knowledge) server: sends SETTINGS, waits for the first HEADERS
  * frame, then replies GOAWAY(NO_ERROR) and closes the socket.
  */
private[ember] object GoAwayServer {

  private val prefaceSize = 24

  private def handle(socket: Socket[IO]): IO[Unit] = {
    def awaitHeaders(acc: ByteVector): IO[Unit] =
      H2Frame.RawFrame.fromByteVector(acc) match {
        case Some((raw, _)) if raw.`type` == H2Frame.Headers.`type` => IO.unit
        case Some((_, rest)) => awaitHeaders(rest)
        case None =>
          socket.read(65536).flatMap {
            case Some(c) => awaitHeaders(acc ++ c.toByteVector)
            case None => IO.unit
          }
      }

    val settings = H2Frame.toByteVector(H2Frame.Settings(0, false, Nil))
    val goAway = H2Frame.toByteVector(H2Frame.GoAway(0, 0, H2Error.NoError.value, None))
    socket.write(Chunk.byteVector(settings)) >>
      socket.readN(prefaceSize) >>
      awaitHeaders(ByteVector.empty) >>
      socket.write(Chunk.byteVector(goAway))
  }

  val resource: Resource[IO, Port] =
    Network[IO].bind(SocketAddress(ip"127.0.0.1", port"0")).flatMap { ss =>
      ss.accept
        .map(s => Stream.eval(handle(s).attempt.void >> s.endOfOutput.attempt.void))
        .parJoinUnbounded
        .compile
        .drain
        .background
        .as(ss.address.asIpUnsafe.port)
    }

  def request(port: Port): Request[IO] =
    Request[IO](uri = Uri.unsafeFromString(s"http://127.0.0.1:$port/"))
      .withAttribute(H2Keys.Http2PriorKnowledge, ())
}
