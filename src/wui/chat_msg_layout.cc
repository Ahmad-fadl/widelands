/*
 * Copyright (C) 2006-2014 by the Widelands Development Team
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 */

#include "wui/chat_msg_layout.h"

#include "chat/chat.h"
#include "graphic/color.h"
#include "logic/constants.h"
#include "logic/player.h"

namespace {

// Returns the hexcolor for the 'player'.
std::string color(const int16_t playern)
{
	if ((playern >= 0) && playern < MAX_PLAYERS) {
		const RGBColor & clr = Widelands::Player::Colors[playern];
		char buf[sizeof("ffffff")];
		snprintf(buf, sizeof(buf), "%.2x%.2x%.2x", clr.r, clr.g, clr.b);
		return buf;
	}
	return "999999";
}

}  // namespace

// TODO(sirver): remove as soon as old text renderer is gone.
std::string format_as_old_richtext(const ChatMessage& chat_message) {
	std::string message = "<p font-color=#33ff33 font-size=9>";

	// Escape richtext characters
	// The goal of this code is two-fold:
	//  1. Assuming an honest game host, we want to prevent the ability of
	//     clients to use richtext.
	//  2. Assuming a malicious host or meta server, we want to reduce the
	//     likelihood that a bug in the richtext renderer can be exploited,
	//     by restricting the set of allowed richtext commands.
	//     Most notably, images are not allowed in richtext at all.
	//
	// Note that we do want host and meta server to send some richtext code,
	// as the ability to send formatted commands is nice for the usability
	// of meta server and dedicated servers, so we're treading a bit of a
	// fine line here.
	std::string sanitized;
	for (std::string::size_type pos = 0; pos < chat_message.msg.size(); ++pos) {
		if (chat_message.msg[pos] == '<') {
			if (chat_message.playern < 0) {
				static const std::string good1 = "</p><p";
				static const std::string good2 = "<br>";
				if (!chat_message.msg.compare(pos, good1.size(), good1)) {
					std::string::size_type nextclose = chat_message.msg.find('>', pos + good1.size());
					if (nextclose != std::string::npos &&
					    (nextclose == pos + good1.size() || chat_message.msg[pos + good1.size()] == ' ')) {
						sanitized += good1;
						pos += good1.size() - 1;
						continue;
					}
				} else if (!chat_message.msg.compare(pos, good2.size(), good2)) {
					sanitized += good2;
					pos += good2.size() - 1;
					continue;
				}
			}

			sanitized += "&lt;";
		} else {
			sanitized += chat_message.msg[pos];
		}
	}

	// time calculation
	char ts[13];
	strftime(ts, sizeof(ts), "[%H:%M] </p>", localtime(&chat_message.time));
	message += ts;

	message += "<p font-size=14 font-face=serif font-color=#";
	message += color(chat_message.playern);

	if (chat_message.recipient.size() && chat_message.sender.size()) {
		// Personal message handling
		if (sanitized.compare(0, 3, "/me")) {
			message += " font-decoration=underline>";
			message += chat_message.sender;
			message += " @ ";
			message += chat_message.recipient;
			message += ":</p><p font-size=14 font-face=serif> ";
			message += sanitized;
		} else {
			message += ">@";
			message += chat_message.recipient;
			message += " >> </p><p font-size=14";
			message += " font-face=serif font-color=#";
			message += color(chat_message.playern);
			message += " font-style=italic> ";
			message += chat_message.sender;
			message += sanitized.substr(3);
		}
	} else {
		// Normal messages handling
		if (!sanitized.compare(0, 3, "/me")) {
			message += " font-style=italic>-> ";
			if (chat_message.sender.size())
				message += chat_message.sender;
			else
				message += "***";
			message += sanitized.substr(3);
		} else if (chat_message.sender.size()) {
			message += " font-decoration=underline>";
			message += chat_message.sender;
			message += ":</p><p font-size=14 font-face=serif> ";
			message += sanitized;
		} else {
			message += " font-weight=bold>*** ";
			message += sanitized;
		}
	}

	// return the formated message
	return message + "<br></p>";
}

// Returns a richtext string that can be displayed to the user.
std::string format_as_richtext(const ChatMessage& chat_message) {
	std::string message = "<p><font color=33ff33 size=9>";

	// Escape richtext characters
	// The goal of this code is two-fold:
	//  1. Assuming an honest game host, we want to prevent the ability of
	//     clients to use richtext.
	//  2. Assuming a malicious host or meta server, we want to reduce the
	//     likelihood that a bug in the richtext renderer can be exploited,
	//     by restricting the set of allowed richtext commands.
	//     Most notably, images are not allowed in richtext at all.
	//
	// Note that we do want host and meta server to send some richtext code,
	// as the ability to send formatted commands is nice for the usability
	// of meta server and dedicated servers, so we're treading a bit of a
	// fine line here.
	std::string sanitized;
	for (std::string::size_type pos = 0; pos < chat_message.msg.size(); ++pos) {
		if (chat_message.msg[pos] == '<') {
			if (chat_message.playern < 0) {
				static const std::string good1 = "</p><p";
				static const std::string good2 = "<br>";
				if (!chat_message.msg.compare(pos, good1.size(), good1)) {
					std::string::size_type nextclose = chat_message.msg.find('>', pos + good1.size());
					if
						(nextclose != std::string::npos &&
						(nextclose == pos + good1.size() || chat_message.msg[pos + good1.size()] == ' '))
					{
						sanitized += good1;
						pos += good1.size() - 1;
						continue;
					}
				} else if (!chat_message.msg.compare(pos, good2.size(), good2)) {
					sanitized += good2;
					pos += good2.size() - 1;
					continue;
				}
			}

			sanitized += "\\<";
		} else {
			sanitized += chat_message.msg[pos];
		}
	}

	// time calculation
	char ts[13];
	strftime(ts, sizeof(ts), "[%H:%M] ", localtime(&chat_message.time));
	message += ts;

	message += "</font><font size=14 face=serif color=";
	message += color(chat_message.playern);

	if (chat_message.recipient.size() && chat_message.sender.size()) {
		// Personal message handling
		if (sanitized.compare(0, 3, "/me")) {
			message += " bold=1>";
			message += chat_message.sender;
			message += " @ ";
			message += chat_message.recipient;
			message += ":</font><font size=14 face=serif shadow=1 color=eeeeee> ";
			message += sanitized;
		} else {
			message += ">@";
			message += chat_message.recipient;
			message += " \\> </font><font size=14";
			message += " face=serif color=";
			message += color(chat_message.playern);
			message += " italic=1 shadow=1> ";
			message += chat_message.sender;
			message += sanitized.substr(3);
		}
	} else {
		// Normal messages handling
		if (!sanitized.compare(0, 3, "/me")) {
			message += " italic=1>-\\> ";
			if (chat_message.sender.size())
				message += chat_message.sender;
			else
				message += "***";
			message += sanitized.substr(3);
		} else if (chat_message.sender.size()) {
			message += " bold=1>";
			message += chat_message.sender;
			message += ":</font><font size=14 face=serif shadow=1 color=eeeeee> ";
			message += sanitized;
		} else {
			message += " bold=1>*** ";
			message += sanitized;
		}
	}

	// return the formated message
	return message + "</font><br></p>";
}
