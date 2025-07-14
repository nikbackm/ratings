// ratings - now for C++! 
//
#include "pch.h"

namespace Utils
{
#define std_string_fmt_impl(strf,resv) va_list ap;va_start(ap,strf); auto resv=fmtv(strf,ap); va_end(ap)

	std::string fmtv(_In_z_ _Printf_format_string_ const char* strf, va_list ap)
	{
		va_list apcopy; va_copy(apcopy, ap);
		size_t const size = vsnprintf(nullptr, 0, strf, ap) + 1; // Extra space for '\0'
		std::string res(size, '\0');
		vsnprintf(&res[0], size, strf, apcopy);
		res.pop_back(); return res; // Remove the trailing '\0' and return
	}

	std::string fmt(_In_z_ _Printf_format_string_ const char* fmtStr, ...)
	{
		std_string_fmt_impl(fmtStr, res);
		return res;
	}

	template <typename T>
	void operator+=(std::vector<T>& a, const std::vector<T>& b) { a.insert(a.end(), b.begin(), b.end()); }

	template <typename T>
	std::string toStr(T value) { return std::to_string(value); }

	template <typename T>
	bool toInt(std::string const& str, T& value)
	{
		errno = 0; char* endPtr; T v;
		if constexpr (std::is_same<T, unsigned long long>::value) v = strtoull(str.c_str(), &endPtr, 10);
		else if constexpr (std::is_same<T, int>::value)           v = strtol(str.c_str(), &endPtr, 10);
		else static_assert(false, "Unsupported number type");
		return (endPtr == str.c_str() || *endPtr != '\0' || errno == ERANGE) ? false : (value = v, true);
	}

	void replaceAll(std::string& str, const std::string& from, const std::string& to)
	{
		if (from.empty()) return;
		for (size_t i = 0; (i = str.find(from, i)) != std::string::npos; i += to.length())
			str.replace(i, from.length(), to);
	}

	std::wstring toWide(int codePage, const char* src, int const len)
	{
		if (len == 0) return std::wstring();
		if (int const cw = MultiByteToWideChar(codePage, 0, src, len, NULL, 0)) {
			std::wstring wstr(cw, '\0');
			if (int const res = MultiByteToWideChar(codePage, 0, src, len, &wstr[0], cw); res == cw)
				return wstr;
		}
		throw std::runtime_error(fmt("MultiByteToWideChar for code page %i failed", codePage));
	}

	std::string toNarrow(int codePage, const wchar_t* src, int const len)
	{
		if (len == 0) return std::string();
		if (int const cb = WideCharToMultiByte(codePage, 0, src, len, NULL, 0, NULL, NULL)) {
			std::string str(cb, '\0');
			if (int const res = WideCharToMultiByte(codePage, 0, src, len, &str[0], cb, NULL, NULL); res == cb)
				return str;
		}
		throw std::runtime_error(fmt("WideCharToMultiByte for code page %i failed", codePage));
	}

	std::wstring utf8ToWide(const char* utf8String, int len) // Used during output.
	{
		return toWide(CP_UTF8, utf8String, len);
	}

	std::string toUtf8(int codePage, std::string const& str)
	{
		auto wstr = toWide(codePage, str.c_str(), str.length());
		return toNarrow(CP_UTF8, wstr.c_str(), wstr.length());
	}

	std::string fromUtf8(int codePage, std::string const& str)
	{
		auto wstr = toWide(CP_UTF8, str.c_str(), str.length());
		return toNarrow(codePage, wstr.c_str(), wstr.length());
	}

	std::string unquote(std::string const& str)
	{
		auto res = str;
		if (!res.empty() && res.back() == '"') res.pop_back();
		if (!res.empty() && res.front() == '"') res.erase(0, 1);
		return res;
	}

	void toLowerCase(std::string& str)
	{
		std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) { return (char)tolower(c); });
	}

	bool enableVTMode() // We assume a Windows 10 edition that supports ANSI.
	{
		if (HANDLE hOut = GetStdHandle(STD_OUTPUT_HANDLE); hOut != INVALID_HANDLE_VALUE)
			if (DWORD dwMode = 0; GetConsoleMode(hOut, &dwMode))
				return SetConsoleMode(hOut, dwMode | ENABLE_VIRTUAL_TERMINAL_PROCESSING);
		return false;
	}

	SYSTEMTIME getLocalTime() { SYSTEMTIME st{}; ::GetLocalTime(&st); return st; }
}
using namespace Utils;

#define S(str) (str).c_str()
#define ESV(str) escSqlVal(str).c_str()

namespace RatingsDefs
{
	using IdValue = unsigned long long;

	enum class DisplayMode {
		column,
		html,
		htmldoc,
		list,
		tabs,
	};

	enum class ColumnType {
		text = 0,
		numeric = 1,
	};

	enum class ColumnSortOrder {
		Asc = 0,
		Desc = 1,
	};
		
	enum class FitWidth {
		off,
		on,
		automatic,
	};

	const char* DefDbName = "ratingsExtended.sqlite";
	const char    OptDelim = '.';
	const char    OptExtDelim = ':';
	const char    Wc = '*';
	const char* WcS = "*";
	const char* LogOp_OR = " OR ";
	const char* LogOp_AND = " AND ";
	const char* NullInput = "NULL"; // Used for inputing NULL values.

	// Escape the SQL value and add the SQL quotes (if needed).
	std::string escSqlVal(std::string str, bool tryToTreatAsNumeric = false)
	{
		if (int intVal; str != NullInput && !(tryToTreatAsNumeric && toInt(str, intVal))) {
			replaceAll(str, "'", "''");
			str = "'" + str + "'";
		}
		return str;
	}

	// Replace our wildcard with SQL's wildcard. Also escape and add SQL quoting if needed.
	std::string likeArg(std::string str, bool tryToTreatAsNumeric = false, bool glob = false)
	{
		if (!glob) std::replace(str.begin(), str.end(), Wc, '%');
		return escSqlVal(str, tryToTreatAsNumeric);
	}

	struct TableInfo {
		TableInfo* parent = nullptr; // Parent table, i.e. table that will need to be included in the current query if this table is. May be null.
		bool used = false;           // Is the table used in the (output) query?
		bool included = false;       // Is the table already included in the query?

		void reset() { *this = TableInfo{}; }
	};

	struct Tables {
		constexpr static int MaxSize = 2;
		TableInfo* tis[MaxSize];

		Tables(TableInfo* ti1 = nullptr, TableInfo* ti2 = nullptr)
			: tis{ti1, ti2} {}

		void reset(TableInfo* ti1 = nullptr, TableInfo* ti2 = nullptr)
		{
			*this = Tables(ti1, ti2);
		}

		TableInfo** begin() { return tis;}

		TableInfo** end() 
		{ 
			int i = 0; while (i < MaxSize && tis[i] != nullptr) ++i;
			return tis + i;
		}
	};

	struct TableInfos {
		TableInfos() {};

		union {
			TableInfo arrView[5] = {};
			#pragma warning(disable : 4201) // nameless struct extension.
			struct {
				// Start tables
				TableInfo ratings;
				TableInfo title_basics;
				TableInfo title_akas;
				TableInfo title_principals; // Also a link table to name_basics
				TableInfo name_basics;
			};
		};
	};
	static_assert(sizeof(TableInfos::arrView) == sizeof(TableInfos), "check arrView size!");

	enum Justify : int { JLeft = 1, JCenter = 0, JRight = -1 }; // For DisplayMode::column

	struct ColumnInfo {
		std::string const nameDef; // Column name or SQL expression (def) for column
		int         const width; // Default width of column, can be overridden.
		Justify     const justify; // Only supports Left and partly Right right now!
		ColumnType  const type; // text or numeric column basically.
		std::string const label; // label for output, useful when nameDef is not a column name.
		bool        const aggr; // Is a group aggregate column?

		ColumnInfo const* lengthColumn = nullptr; // Will be set only if the column has a length column added.
		const char*       collation = nullptr; // Only set if not using the default (usually BINARY)
		mutable Tables    tables; // Tables needed by nameDef in the query, varies with startTable(action).

		mutable int  sWidth = -1; // Width set by the -s option.
		mutable bool usedInQuery = false; // Tells if column is references anywhere in the query.
		mutable bool usedInResult = false; // Tells if column is a result (i.e. SELECT:ed) column.

		ColumnInfo(std::string const& nameDef, int width, Justify j, ColumnType ct, std::string const& label, bool aggr, Tables t) :
			nameDef(nameDef), width(width), justify(j), type(ct), label(label), aggr(aggr), tables(t) {}

		std::string const& labelName() const { return label.empty() ? nameDef : label; }

		std::string getLikeArg(std::string val, bool glob = false) const
		{
			return likeArg(std::move(val), type == ColumnType::numeric, glob);
		}
	};

	// A collection of columns and integer data according to ColumnsDataKind.
	using Columns = std::vector<std::pair<ColumnInfo const*, int>>;

	enum class ColumnsDataKind { none, width, sortOrder };
}
using namespace RatingsDefs;

class OptionParser {
	std::string       m_option;
	unsigned          m_optionIndex = 0;
	const char* const m_type;
	const char        m_delim;
public:
	OptionParser(std::string const& value, const char* type = "option", char delim = OptDelim)
		: m_option(value), m_type(type), m_delim(delim)
	{}

	bool empty() const { return m_option.length() <= m_optionIndex; }
	// These two allows clients to push a read value back. (Safe thanks to empty())
	unsigned position() const { return m_optionIndex; }
	void     setPosition(unsigned pos) { m_optionIndex = pos; }

	bool getNext(std::string& next)
	{
		if (empty()) return false;
		const int EscapeChar = '!';
		bool escOn = false;

		std::string str;
		do {
			char const c = m_option[m_optionIndex++];
			if (c == EscapeChar) {
				if (escOn) { str.push_back(EscapeChar); }
				escOn = !escOn;
			}
			else if (c == m_delim) {
				if (escOn) {
					str.push_back(m_delim);
					escOn = false;
				}
				else {
					break; // delim found
				}
			}
			else {
				if (escOn) throw std::invalid_argument("Illegal escape sequence");
				str.push_back(c);
			}
		} while (!empty());
		if (escOn) throw std::invalid_argument("Incomplete escape sequence");
		if (str.empty()) throw std::invalid_argument("Empty option values not allowed");

		next = std::move(str);
		return true;
	}

	__declspec(noreturn) void throwError()
	{
		throw std::invalid_argument(fmt("Faulty %s value: %s", m_type, S(m_option)));
	}

	std::string getNext()
	{
		if (std::string value; getNext(value)) return value;
		throwError();
	}

	int nextInt()
	{
		auto val = getNext();
		if (int ival; toInt(val, ival)) return ival;
		throwError();
	}

	std::string nextIntAsStr() { return toStr(nextInt()); }
};

class Output {
	const int Utf8Encoding = 65001;

	DWORD        m_consoleMode = 0;
	int    const m_consoleCodePage = GetConsoleCP();
	HANDLE const m_stdOutHandle = GetStdHandle(STD_OUTPUT_HANDLE);
	bool   const m_stdOutIsConsole = m_stdOutHandle != NULL && GetConsoleMode(m_stdOutHandle, &m_consoleMode);
	int          m_encoding = Utf8Encoding;
	// Got missing WriteConsole output with 32K buffer! 20K seems ok so far... but uses 10K to avoid analyse warning..
	// 32K seems to work at home with Win10 though, but not at work with Win7.
	static const int BufSize = 1000 * 10;
	mutable char m_buffer[BufSize];
	mutable int  m_bufPos = 0;
	mutable bool m_error = false;

public:
	bool stdOutIsConsole() const { return m_stdOutIsConsole; }

	void setEncoding(int encoding) { m_encoding = encoding; }

	bool error() const { return m_error; }

	void writeUtf8Bom() const
	{
		if (m_encoding == Utf8Encoding && !m_stdOutIsConsole) {
			const unsigned char bom[] = { 0xEF, 0xBB, 0xBF };
			write((const char*)&bom[0], sizeof(bom));
		}
	}

	void write(const char* str, int len) const
	{
		if (BufSize < len) {
			flush();
			doWrite(str, len);
		}
		else {
			if ((BufSize - m_bufPos) < len) flush();
			_ASSERT(len <= (BufSize - m_bufPos));
			memcpy(&m_buffer[m_bufPos], str, len);
			m_bufPos += len;
		}
	}

	void write(char c) const
	{
		if (BufSize == m_bufPos) {
			flush(); Assert(m_bufPos == 0);
		}
		m_buffer[m_bufPos++] = c;
	}

	void write(std::string const& str) const
	{
		write(str.c_str(), str.length());
	}

	void write(const char* str) const
	{
		write(str, strlen(str));
	}

	void writeUtf8Width(const char* str, int width, Justify justify) const
	{ // PRE: str contains only complete utf-8 code points.
		int writtenChars = 0;

		if (justify == JRight) { // OBS! Does not work for utf-8 strings with multibyte chars! Mainly with numeric columns for now!
			for (int i = strlen(str); i < width; ++i)
			{
				write(' ');
				++writtenChars;
			}
		}

		for (int i = 0; str[i] != '\0' && writtenChars < width; /*inc in body*/) {
			if ((str[i] & 0x80) == 0) { // 0xxx xxxx - Single utf8 byte
				write(str[i]);
				i += 1;
			}
			else if ((str[i] & 0xC0) == 0xC0) { // 11xx xxxx - utf8 start byte
				int n = 1;
				while ((str[i + n] & 0xC0) == 0x80) ++n; // 10xx xxxx - continuation byte
				if (n == 1) throw std::invalid_argument("No utf-8 continuation byte(s)!");
				if (4 < n) throw std::invalid_argument("Too many utf-8 continuation bytes!");
				write(&str[i], n);
				i += n;
			}
			else {
				throw std::invalid_argument("Invalid utf-8 byte!");
			}
			// The above is ratingsle more complex that would be needed, as we must make sure
			// to write complete utf-8 code points. Would not do if the buffer were flushed while
			// we wrote a middle or starting utf-8 byte! Well, will work when writing to file,
			// but not when writing to the console.
			// Note: We assume every Unicode code point corresponds to an actual visible character.
			// Does not take combining characters and such into account.
			++writtenChars;
		}

		while (writtenChars < width) {
			write(' ');
			++writtenChars;
		}
	}

	void writeHtml(const char* z) const
	{
		int i;
		while (*z) {
			for (i = 0; z[i] != '\0'
				&& z[i] != '<'
				&& z[i] != '&'
				&& z[i] != '>'
				&& z[i] != '\"'
				&& z[i] != '\'';
				i++) {
			}

			if (i > 0) write(z, i);

			if (z[i] == '<') { write("&lt;"); }
			else if (z[i] == '&') { write("&amp;"); }
			else if (z[i] == '>') { write("&gt;"); }
			else if (z[i] == '\"') { write("&quot;"); }
			else if (z[i] == '\'') { write("&#39;"); }
			else { break; }

			z += i + 1;
		}
	}

	void doWrite(const char* const str, int const len) const
	{
		if (len == 0) return;
		if (m_stdOutIsConsole) {
			auto ws = utf8ToWide(str, len);
			if (DWORD nw; !WriteConsole(m_stdOutHandle, ws.c_str(), ws.length(), &nw, 0))
				throw std::runtime_error("WriteConsole failed with error code: " + toStr(GetLastError()));
		}
		else {
			int res;
			if (m_encoding == Utf8Encoding) {
				res = fwrite(str, len, 1, stdout);
			}
			else {
				auto wstr = Utils::utf8ToWide(str, len);
				auto encStr = Utils::toNarrow(m_encoding, wstr.c_str(), wstr.length());
				res = fwrite(encStr.c_str(), encStr.length(), 1, stdout);
			}
			if (res != 1) {
				m_error = true;
				throw std::runtime_error("fwrite failed to write all data, errno: " + toStr(errno));
			}
		}
	}

	void flush() const
	{
		// Reset buffer before calling doWrite so we don't end up with further write errors during
		// error propagation in case it fails. Only the first failure is relevant.
		auto len = m_bufPos; m_bufPos = 0;
		doWrite(m_buffer, len);
	}

	void flushNoThrow() const
	{
		try {
			flush();
		}
		catch (std::exception& ex) {
			fprintf(stderr, "\nflushOutput failed: %s\n", ex.what());
		}
	}
};

struct SqliteCloser { void operator()(sqlite3* p) const { sqlite3_close(p); } };

enum class ExQ { None = 0, Graph = 1, VMCode = 2, Raw = 3 };

class Ratings {
	TableInfos m_tableInfos;
	std::map<std::string, ColumnInfo> m_columnInfos; // short name => ColumnInfo. NO operator names! E.g. asc,desc,lt,eq.
	mutable decltype(m_columnInfos) m_ancis; // Holds ColumnInfo:s for dynamically added "actual name" columns.
	std::unique_ptr<sqlite3, SqliteCloser> m_conn;
	Output m_output;
	int const consoleCodePage = GetConsoleCP();

	bool m_headerOn = true;
	FitWidth m_fitWidth = FitWidth::automatic;
	int  m_fitWidthValue = 230;
	bool m_selectDistinct = false;
	bool m_showQuery = false;
	bool m_showDefaults = false;
	ExQ m_explainQuery = ExQ::None;
	bool m_showNumberOfRows = false;
	DisplayMode m_displayMode = DisplayMode::column;
	std::string m_listSep = "|";
	std::string m_colSep = "  ";
	int m_colSepSize = 2; // Avoid recalculating during output since contains character count and not code-point count.
	std::string m_dbPath; // Path to LITT db file
	Columns m_orderBy; // Overrides the default action order.
	int m_limit = 0;
	int m_offset = 0;
	Columns m_selectedColumns; // Overrides the default action columns.
	Columns m_additionalColumns; // Added to the action or overridden columns.
	std::string m_whereBase, m_whereAdditional;
	std::string m_having;
	std::string m_action;
	std::vector<std::string> m_args;
	int m_rowCount = 0; // The number of rows printed so far.

	ColumnInfo& ci(std::string const& sn, std::string const& nameDef, int width, ColumnType ct, Tables t, std::string const& label, bool aggr = false)
	{
		if (auto res = m_columnInfos.try_emplace(sn, nameDef, abs(width), width > 0 ? JLeft : JRight, ct, label, aggr, t); res.second)
			return res.first->second;
		throw std::logic_error("Duplicate short name: " + sn);
	}

	ColumnInfo& ciText(std::string const& sn, std::string const& nameDef, int width, Tables tables, std::string const& label = "")
	{
		return ci(sn, nameDef, width, ColumnType::text, tables, label);
	}

	ColumnInfo& ciNum(std::string const& sn, std::string const& nameDef, int width, Tables tables, std::string const& label = "")
	{
		return ci(sn, nameDef, width, ColumnType::numeric, tables, label);
	}

	ColumnInfo& ciTextL(std::string const& sn, std::string const& nameDef, int width, Tables tables, const char* collation, std::string const& label = "")
	{
		auto& ci = ciText(sn, nameDef, width, tables, label);
		auto const labelLength = "l_" + sn;
		ci.lengthColumn = &ciNum(sn + "l", "length(" + nameDef + ")", labelLength.length(), tables, labelLength);
		ci.collation = collation;
		return ci;
	}

	ColumnInfo& ciAggr(std::string const& sn, std::string const& def, int width, Tables tables, std::string const& label)
	{
		return ci(sn, def, width, ColumnType::numeric, tables, label, true);
	}

#define Q(str) "\"" str "\""
#define CAST(to, what) "CAST(" what " AS " to ")"

public:
	Ratings(int argc, char** argv)
	{
		auto const CNoCase = "NOCASE";
		auto& t = m_tableInfos;
	
		ciText("tc", "tconst", 10, Tables(), Q("Title key"));
		ciNum("ra", Q("Your Rating"), 6, Tables(&t.ratings), "Rating");
		ciTextL("ti", "ratings.Title", 60, Tables(&t.ratings), CNoCase);
		ciText("dr", Q("Date Rated"), 10, Tables(&t.ratings));
		ciText("url", "URL", 38, Tables(&t.ratings), Q("IMDB url"));
		ciText("ty", Q("Title Type"), 20, Tables(&t.ratings), "Type");
		ciNum("ri", Q("IMDB Rating"), 9, Tables(&t.ratings), Q("IMDB Rat."));
		ciNum("rtm", Q("Runtime (mins)"), 7, Tables(&t.ratings), "Minutes");
		ciNum("rth", "(" Q("Runtime (mins)") "+59)/60", 5, Tables(&t.ratings), "Hours");
		ciNum("rtq", "((" Q("Runtime (mins)") "+ 7)/15)*15", 5, Tables(&t.ratings), "Min15");
		ciNum("ye", "Year", 4, Tables(&t.ratings));
		ciTextL("ge", "Genres", 50, Tables(&t.ratings), CNoCase);
		ciNum("nv", Q("Num Votes"), 9, Tables(&t.ratings), "Votes");
		ciText("rd", Q("Release Date"), 10, Tables(&t.ratings), "Released");
		ciTextL("di", "Directors", 50, Tables(&t.ratings), CNoCase);
		ciTextL("tiye", "Title || ' (' || Year || ')'", 66, Tables(&t.ratings), CNoCase, Q("Title (Year)"));
		ciNum("yr", CAST("INTEGER","SUBSTR(\"Date Rated\",1,4)"), 6, Tables(&t.ratings), Q("Year Rated"));
		ciAggr("rc", "COUNT(\"Your Rating\")", -8, Tables(&t.ratings), Q("#ratings"));
		
		ciText("nc", "nconst", 10, Tables(), Q("Name key"));
		ciText("pn", "primaryName", 30, Tables(&t.name_basics), Q("Primary Name"));
		ciNum("by", "birthYear", 4, Tables(&t.name_basics), Q("Birth Year"));
		ciNum("dy", "deathYear", 4, Tables(&t.name_basics), Q("Death Year"));
		ciText("pp", "primaryProfession", 30, Tables(&t.name_basics), Q("Primary Profession"));
		ciText("kt", "knownForTitles", 30, Tables(&t.name_basics), Q("Known For Titles"));

		ciNum("tpo", "ordering", 2, Tables(&t.title_principals), "Ordering");
		ciText("cat", "category", 20, Tables(&t.title_principals), "Category");
		ciText("job", "job", 20, Tables(&t.title_principals), "Job");
		ciText("ch", "characters", 30, Tables(&t.title_principals), "Characters");

		ciText("pt", "primaryTitle", 30, Tables(&t.title_basics), Q("Primary Title"));
		ciText("ot", "originalTitle", 30, Tables(&t.title_basics), Q("Original Title"));
		ciNum("isad", "isAdult", 3, Tables(&t.title_basics), Q("Is Adult"));
		ciNum("sy", "startYear", 4, Tables(&t.title_basics), Q("Start Year"));
		ciNum("ey", "endYear", 4, Tables(&t.title_basics), "End Year");

		ciText("tia", "title_akas.title", 30, Tables(&t.title_akas), "Title");
		ciText("rg", "region", 4, Tables(&t.title_akas), "Region");
		ciText("la", "language", 4, Tables(&t.title_akas), "Language");
		ciText("tya", "types", 20, Tables(&t.title_akas), "Types");
		ciText("at", "attributes", 20, Tables(&t.title_akas), "Attributes");
		ciNum("isot", "isOriginalTitle", 3, Tables(&t.title_akas), Q("Is Original Title"));

		if (m_output.stdOutIsConsole()) {
			CONSOLE_SCREEN_BUFFER_INFO csbi{}; GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi);
			m_fitWidthValue = csbi.srWindow.Right - csbi.srWindow.Left;
		}

		for (int i = 1; i < argc; ++i) {
			if (argv[i][0] == '-' && argv[i][1] != '\0') { // A stand-alone '-' can be used as an (action) argument.
				auto const opt = argv[i][1];
				auto const val = std::string(argv[i][2] != '\0' ? &argv[i][2] : "");
				switch (opt) {
				case 'd':
					if (val == "col" || val == "column") m_displayMode = DisplayMode::column;
					else if (val == "html")              m_displayMode = DisplayMode::html;
					else if (val == "htmldoc")           m_displayMode = DisplayMode::htmldoc;
					else if (val == "tabs")              m_displayMode = DisplayMode::tabs;
					else if (val.substr(0, 4) == "list" && (val.length() == 4 || val[4] == ':')) {
						m_displayMode = DisplayMode::list;
						if (4 < val.length()) m_listSep = toUtf8(val.substr(5));
					}
					else throw std::invalid_argument("Invalid display mode: " + val);
					break;
				case 'e':
					if (int enc; toInt(val, enc)) m_output.setEncoding(enc);
					else throw std::invalid_argument("Invalid encoding value: " + val);
					break;
				case 'f': {
					OptionParser fval(val);
					auto v = fval.empty() ? "on" : fval.getNext();
					if (v == "on" || toInt(v, m_fitWidthValue)) m_fitWidth = FitWidth::on;
					else if (v == "off")                             m_fitWidth = FitWidth::off;
					else if (v == "auto")                            m_fitWidth = FitWidth::automatic;
					else throw std::invalid_argument("Invalid fit width value: " + val);
					break; }
				case 'h':
					if (val == "on" || val == "") m_headerOn = true;
					else if (val == "off")         m_headerOn = false;
					else throw std::invalid_argument("Invalid header value: " + val);
					break;
				case 'o':
					m_orderBy = getColumns(val, ColumnsDataKind::sortOrder, true, true);
					break;
				case 'c':
					m_selectedColumns = getColumns(val, ColumnsDataKind::width, true);
					break;
				case 'l':
					m_dbPath = val;
					break;
				case 'a': case 'i': {
					auto const acs = getColumns(val, ColumnsDataKind::width, true);
					m_additionalColumns += acs;
					if (opt == 'i') for (auto const& c : acs) appendToWhere(c.first->labelName() + " IS NOT NULL");
					break; }
				case 'w':
					m_whereAdditional = appendConditions(LogOp_OR, m_whereAdditional, getWhereCondition(val));
					break;
				case 's':
					for (auto& c : getColumns(val, ColumnsDataKind::width, false)) c.first->sWidth = c.second;
					break;
				case 'q':
					m_showQuery = true;
					m_showDefaults = (val == "d");
					break;
				case 'u':
					m_selectDistinct = true;
					break;
				case 'x':
					if (!val.empty() && (val[0] < '0' || '3' < val[0])) throw std::invalid_argument("Invalid explain value: " + val);
					m_explainQuery = (val.empty() ? ExQ::Graph : ExQ(val[0] - '0'));
					break;
				case 'n':
					m_showNumberOfRows = true;
					break;
				case '-': { // Extended option 
					OptionParser extVal(val, "extended option", OptExtDelim);
					auto const extName = extVal.getNext();
					if (extName == "cons") {
						m_consRowMinCount = extVal.nextInt();
						for (std::string colName = extVal.getNext();;) {
							colName = getColumnName(colName);
							ConsRowColumnInfo col;
							col.name = colName; colName.clear();
							col.index = -1; // Will be set when we get the first callback
							col.matchMethod = ConsRowMatchMethod::columnValue;
							col.charCmpCount = 0;
							std::string mm;
							if (extVal.getNext(mm)) {
								if (mm == "regex" || mm == "re" || mm == "ren") {
									col.matchMethod = (mm == "ren") ? ConsRowMatchMethod::regExNot : ConsRowMatchMethod::regEx;
									col.re = getRegex(extVal.getNext());
									extVal.getNext(colName);
								}
								else if (toInt(mm, col.charCmpCount)) {
									extVal.getNext(colName);
								}
								else {
									colName = mm; // use as name next iteration
								}
							}
							m_consRowColumns.push_back(col);
							if (colName.empty()) break;
						}
					}
					else if (extName == "ansi") {
						m_ansiEnabled = true; // On by default when using ansi option
						auto nextColor = [&]() {
							auto val = extVal.getNext();
							return (val[0] == '\x1b') ? val : "\x1b[" + val;
						};
						for (std::string subOpt; extVal.getNext(subOpt); ) {
							toLowerCase(subOpt);
							if (subOpt == "off") {
								m_ansiEnabled = false;
							}
							else if (subOpt == "defc") {
								m_ansiDefColor = nextColor();
							}
							else if (subOpt == "colc") {
								AnsiColumnColor acc;
								acc.colName = getColumnName(extVal.getNext());
								acc.ansiColor = nextColor();
								m_ansiColColors.push_back(acc);
							}
							else if (subOpt == "valc") {
								AnsiValueColor avc;
								avc.colName = getColumnName(extVal.getNext());
								avc.rowValueRegEx = getRegex(extVal.getNext());
								auto coloredCols = OptionParser(extVal.getNext(), "column", OptDelim);
								do {
									avc.coloredColumns.push_back(getColumnName(coloredCols.getNext()));
								} while (!coloredCols.empty());
								avc.ansiColor = nextColor();
								m_ansiValueColors.push_back(avc);
							}
							else {
								throw std::invalid_argument("Unrecognized ansi sub-option: " + subOpt);
							}
						}
					}
					else if (extName == "limit") {
						m_limit = extVal.nextInt();
					}
					else if (extName == "offset") {
						m_offset = extVal.nextInt();
					}
					else if (extName == "colsep") {
						m_colSep = toUtf8(extVal.empty() ? std::string() : extVal.getNext());
						m_colSepSize = Utils::utf8ToWide(m_colSep.c_str(), m_colSep.length()).length(); // Assumes UTF-16 length = #glyphs!
					}
					else throw std::invalid_argument("Unrecognized extended option: " + extName);
					break;
				}
				default:
					throw std::invalid_argument(std::string("Unrecognized option: ") + argv[i]);
				}
			}
			else {
				if (m_action.empty()) {
					m_action = argv[i];
				}
				else {
					m_args.push_back(argv[i]);
				}
			}
		}

		if (m_dbPath.empty()) {
			char mydocs[MAX_PATH];
			m_dbPath = GetEnvironmentVariableA("MYDOCS", mydocs, MAX_PATH)
				? fmt("%s\\ratings\\%s", mydocs, DefDbName) : DefDbName;
		}
		if (GetFileAttributesA(m_dbPath.c_str()) == -1)
			throw std::invalid_argument("Cannot find: " + m_dbPath);

		sqlite3* conn = nullptr;
		if (int rc = sqlite3_open(m_dbPath.c_str(), &conn); m_conn.reset(conn), rc != SQLITE_OK)
			throw std::runtime_error(fmt("Cannot open database: %s", sqlite3_errmsg(conn)));
	}

	ColumnInfo const* getColumn(std::string const& sn, bool allowActualName = false) const
	{
		if (auto it = m_columnInfos.find(sn); it != m_columnInfos.end()) return &it->second;
		if (allowActualName) return &m_ancis.try_emplace(sn, sn, (int)sn.length(), JLeft, ColumnType::numeric, sn, false, Tables()).first->second;
		throw std::invalid_argument("Invalid short column name: " + sn);
	}

	Columns getColumns(std::string const& sns, ColumnsDataKind kind, bool usedInQuery, bool allowActualName = false) const
	{
		Columns res;
		OptionParser opts(sns);
		for (std::string sn; opts.getNext(sn); ) {
			auto column = getColumn(sn, allowActualName);
			if (usedInQuery) column->usedInQuery = true;

			// Now get the optional data (sortOrder/width).
			int data = -1;
			auto const curOptPos = opts.position();
			if (opts.getNext(sn)) {
				if (kind == ColumnsDataKind::sortOrder) {
					if (sn == "asc")       data = (int)ColumnSortOrder::Asc;
					else if (sn == "desc") data = (int)ColumnSortOrder::Desc;
				}
				else if (kind == ColumnsDataKind::width) {
					toInt(sn, data);
				}
			}
			if (data < 0) { // Data not read or not valid
				opts.setPosition(curOptPos); // if invalid data value was read, undo read.
				switch (kind) {//  provide default values.
				case ColumnsDataKind::width:     data = column->width; break;
				case ColumnsDataKind::sortOrder: data = (int)ColumnSortOrder::Asc; break;
				}
			}
			res.emplace_back(column, data);
		} // for
		return res;
	}

	std::string getColumnName(std::string const snOrName)
	{
		auto ci = m_columnInfos.find(snOrName);
		return ci != m_columnInfos.end() ? unquote(ci->second.labelName()) : snOrName;
	}

	std::regex getRegex(std::string const& reVal)
	{
		namespace r = std::regex_constants;
		return std::regex(toUtf8(reVal), r::ECMAScript | r::optimize | r::nosubs);
	}

	std::string getWhereCondition(std::string const& value) // Will also update included columns!
	{
		OptionParser opts(value, "where");
		std::string whereCond, havingCond;

		for (std::string sn; opts.getNext(sn); ) {
			auto col = getColumn(sn);
			col->usedInQuery = true;

			auto val = opts.getNext(); // Either a value or an operation for the value coming up.
			bool glob = false; // Must not replace wildcard if globbing.
			std::string oper;
			if (val == "lt")  oper = "<";
			else if (val == "lte") oper = "<=";
			else if (val == "gt")  oper = ">";
			else if (val == "gte") oper = ">=";
			else if (val == "eq")  oper = "=";
			else if (val == "neq") oper = "notlike";
			else if (val == "glob") oper = (glob = true, "GLOB");
			else if (val == "nglob") oper = (glob = true, "NOT GLOB");
			else if (val == "isnull" || val == "notnull" || val == "isempty" || val == "range" || val == "nrange") oper = val;
			else if (val == "and" || val == "or" || val == "nand" || val == "nor") oper = val;

			if (oper.empty()) oper = "LIKE"; // And (current) val is the operand
			else if (oper != "isnull" && oper != "notnull" && oper != "isempty") val = opts.getNext();

			auto getOperand = [this, col, glob](std::string val) -> std::string {
				if (auto valCol = m_columnInfos.find(val); valCol != m_columnInfos.end()) {
					valCol->second.usedInQuery = true;
					return valCol->second.nameDef;
				}
				return col->getLikeArg(std::move(val), glob);
			};
			val = getOperand(val);

			auto const& cn = col->nameDef;
			std::string cond;
			if (oper == "notlike") cond = "ifnull(" + cn + ", '') NOT LIKE " + val;
			else if (oper == "isnull")  cond = cn + " IS NULL";
			else if (oper == "notnull") cond = cn + " IS NOT NULL";
			else if (oper == "isempty") cond = cn + " = ''";
			else if (oper == "range")   cond = val + " <= " + cn + " AND " + cn + " <= " + getOperand(opts.getNext());
			else if (oper == "nrange")  cond = "NOT (" + val + " <= " + cn + " AND " + cn + " <= " + getOperand(opts.getNext()) + ")";
			else if (oper == "and" || oper == "or" || oper == "nand" || oper == "nor") {
				cond = "(";
				bool nott = (oper[0] == 'n');
				for (;;) {
					cond.append("ifnull(" + cn + ", '')"
						+ (val[0] == '\'' ? (nott ? " NOT LIKE " : " LIKE ") : (nott ? " <> " : " = "))
						+ val);
					if (!opts.getNext(val)) break;
					cond.append(oper.back() == 'd' ? LogOp_AND : LogOp_OR);
					val = getOperand(val);
				}
				cond.append(")");
			}
			else {
				cond = cn + " " + oper + " " + val;
			}
			auto& toCond = col->aggr ? havingCond : whereCond;
			toCond = toCond.empty() ? cond : (toCond + LogOp_AND + cond);
		}

		appendToHaving(LogOp_OR, havingCond); // HACK: always merged, not returned. Works for now though!
		return whereCond;
	}

	static std::string parseCountCondition(std::string const& name, std::string const& value)
	{
		OptionParser opts(value, "count condition");
		auto val = opts.getNext();
		int iVal;
		if (toInt(val, iVal)) { return name + " >= " + val; }
		else if (val == "lt") { return name + " < " + opts.nextIntAsStr(); }
		else if (val == "gt") { return name + " > " + opts.nextIntAsStr(); }
		else if (val == "eq") { return name + " = " + opts.nextIntAsStr(); }
		else if (val == "range") {
			auto r1 = opts.nextIntAsStr(); auto r2 = opts.nextIntAsStr();
			return r1 + " <= " + name + " AND " + name + " <= " + r2;
		}
		else {
			throw std::invalid_argument("Invalid operator " + val + " in count condition value: " + value);
		}
	}

	static std::string appendConditions(const char* logicalOp, std::string const& cond, std::string const& cond2)
	{
		if (cond.empty()) return cond2;
		if (cond2.empty()) return cond;
		return "(" + cond + ")" + logicalOp + "(" + cond2 + ")";
	}

	void appendToHaving(const char* logicalOp, std::string const& condition)
	{
		m_having = appendConditions(logicalOp, m_having, condition);
	}

	void appendToWhere(std::string const& condition)
	{
		m_whereBase = appendConditions(LogOp_AND, m_whereBase, condition);
	}

	std::string whereCondition() const { return appendConditions(LogOp_AND, m_whereBase, m_whereAdditional); }

	// Needed when listing output multiple times in same LITT session (like when listing items for book input!)
	// Note that usedInResult is used/checked and reset in addOrderBy. Cannot do that here since addAuxTables
	// is called multiple times to generate a single query in some cases.
	void resetTablesAndColumns()
	{
		for (auto& ti : m_tableInfos.arrView) ti.reset();
		for (auto& node : m_columnInfos) node.second.usedInQuery = false;
	}

	void resetListingData(std::string const& whereCondition) // For use from listings during add actions.
	{
		resetTablesAndColumns();
		m_selectedColumns.clear(); m_orderBy.clear(); m_additionalColumns.clear();
		m_whereBase = getWhereCondition(whereCondition); m_whereAdditional.clear();
	}

	void addActionWhereCondition(const char* sn, std::string const& cond)
	{
		if (!cond.empty()) {
			auto val = cond;
			auto col = getColumn(sn);
			col->usedInQuery = true;
			appendToWhere(col->nameDef + " LIKE " + col->getLikeArg(val));
		}
	}

	bool hasArg(unsigned index) { return index < m_args.size(); }

	std::invalid_argument argEx(unsigned index, const char* name) const
	{
		return std::invalid_argument(fmt("Invalid %s value: %s", name, S(m_args[index])));
	}

	std::string arg(unsigned index, const char* def = "") const
	{
		return index < m_args.size() ? m_args[index] : def;
	}

	int intarg(unsigned index, const char* name, int def) const
	{
		int val; return index < m_args.size()
			? toInt(m_args[index], val) ? val : throw argEx(index, name)
			: def;
	}

	std::string toUtf8(std::string const& str) const { return Utils::toUtf8(consoleCodePage, str); }
	std::string fromUtf8(std::string const& str) const { return Utils::fromUtf8(consoleCodePage, str); }
	std::string encodeSqlFromInput(std::string const& sql) const { return toUtf8(sql); }

	enum class Table { // The main tables, used to select listing source.
		ratings,
		title_basics,
		title_akas,
		title_principals,
		name_basics,
	};

	struct TableData {
		const char* tableName;
		TableInfo& tableInfo;
	};

	TableData m_tableData[5] = { // Same order as Table!
		{ "ratings", m_tableInfos.ratings },
		{ "title_basics", m_tableInfos.title_basics },
		{ "title_akas", m_tableInfos.title_akas },
		{ "title_principals", m_tableInfos.title_principals },
		{ "name_basics", m_tableInfos.name_basics},
	};

	const char* getTableName(Table table) const
	{
		return m_tableData[(int)table].tableName;
	}

	TableInfo& getTableInfo(Table table)
	{
		return m_tableData[(int)table].tableInfo;
	}

	enum class SelectOption { normal, distinct };

	struct QueryBuilder {
		std::string m_query;

		QueryBuilder(const char* sql = nullptr)
		{
			m_query.reserve(5000);
			if (sql != nullptr) m_query.append(sql);
		}

		void a(const char* str) { m_query.append(str); }
		void a(std::string const& str) { m_query.append(str); }

		void add(const char* line)
		{
			m_query.append("\n").append(line);
		}

		void add(std::string const& line) { add(line.c_str()); }

		void adf(_In_z_ _Printf_format_string_ const char* fmtStr, ...)
		{
			std_string_fmt_impl(fmtStr, res);
			add(res);
		}

		void aIf(std::string const& line, bool cond) { if (cond) add(line); }
	};

	struct ColumnSetting {
		int     width;
		Justify justify;
		ColumnSetting(int w, Justify j = JLeft) : width(w), justify(j) {}
	};

	struct OutputQuery : QueryBuilder {
		Columns m_orderBy;
		Ratings& ratings;
		mutable std::vector<ColumnSetting> columnSettings; // Only set (or used at least!) for column mode.

		OutputQuery(Ratings& ratings) : ratings(ratings) {} // For queries built piecemeally.

		OutputQuery(Ratings& ratings, const char* sql) : OutputQuery(ratings) // For custom SQL queries.
		{
			addExplain(); a(sql);
		}

		OutputQuery(Ratings& ratings, const char* defColumns, const char* from, const char* defOrderBy, SelectOption selOpt = SelectOption::normal)
			: OutputQuery(ratings, defColumns, nullptr/*with*/, from, defOrderBy, selOpt) {}

		OutputQuery(Ratings& ratings, const char* defColumns, const char* with, const char* from, const char* defOrderBy, SelectOption selOpt = SelectOption::normal)
			: OutputQuery(ratings)
		{
			if (ratings.m_showDefaults) printf("defColumns: %s\ndefOrderBy: %s\n\n", defColumns, defOrderBy);
			addSelect(with, selOpt);
			addResultColums(defColumns);
			m_query.append("\nFROM ").append(from);
			initOrderBy(defOrderBy);
		}

		void addExplain()
		{
			if (ratings.m_explainQuery != ExQ::None) {
				m_query.append("EXPLAIN ");
				if (ratings.m_explainQuery != ExQ::VMCode) m_query.append("QUERY PLAN ");
			}
		}

		void addSelect(SelectOption selOpt = SelectOption::normal) { addSelect(nullptr, selOpt); }

		void addSelect(const char* with, SelectOption selOpt = SelectOption::normal) // Also adds EXPLAIN and DISTINCT
		{
			addExplain();
			if (with != nullptr) m_query.append("WITH\n").append(with).append("\n");
			m_query.append("SELECT ");
			if (ratings.m_selectDistinct || selOpt == SelectOption::distinct) m_query.append("DISTINCT ");
		}

		void addResultColums(const char* defColumns)
		{
			auto selCols = ratings.m_selectedColumns.empty() ? ratings.getColumns(defColumns, ColumnsDataKind::width, true) : ratings.m_selectedColumns;
			selCols += ratings.m_additionalColumns;
			for (unsigned i = 0; i < selCols.size(); ++i) {
				auto ci = selCols[i].first;
				ci->usedInResult = true;
				if (i != 0) m_query.append(",");
				m_query.append(ci->nameDef);
				if (!ci->label.empty()) m_query.append(" AS ").append(ci->label);
				if (ratings.m_displayMode == DisplayMode::column) {
					columnSettings.emplace_back(ci->sWidth >= 0 ? ci->sWidth : selCols[i].second, ci->justify);
				}
			}
		}

		void initOrderBy(const char* defOrderBy, bool allowActualNames = false)
		{
			auto asc = [](Columns cols) { for (auto& c : cols) { c.second = (int)ColumnSortOrder::Asc; } return cols; };
			m_orderBy = ratings.m_orderBy.empty()
				? (ratings.m_selectedColumns.empty()
					? ratings.getColumns(defOrderBy, ColumnsDataKind::sortOrder, true, allowActualNames)
					: asc(ratings.m_selectedColumns))
				: ratings.m_orderBy;
		}

		void addWhere(const char* prefixKeyword = "WHERE")
		{
			if (auto wc = ratings.whereCondition(); !wc.empty()) adf("%s %s", prefixKeyword, S(wc));
		}

		void addHaving(const char* prefix = "")
		{
			if (!ratings.m_having.empty()) adf("%sHAVING %s", prefix, S(ratings.m_having));
		}

		void xad(std::string const& line)
		{
			m_query.append("\n"); addExplain(); m_query.append(line);
		}

		void xaf(_In_z_ _Printf_format_string_ const char* fmtStr, ...)
		{
			std_string_fmt_impl(fmtStr, res);
			xad(res);
		}

		// NOTE: We assume the tableInfos are reset whenever startTable is changed to something else than the last call.
		void initTablesAndColumns(Table const startTable)
		{
			// First determine tables relationships according to start table and which table link/id fields are taken from
			//
			auto& t = ratings.m_tableInfos;
			auto& tc = ratings.getColumn("tc")->tables;
			auto& nc = ratings.getColumn("nc")->tables;

			t.name_basics.parent = &t.title_principals;

			switch (startTable) {
			case Table::ratings:
			case Table::title_basics:
			case Table::title_akas:
				tc.reset(); // Included in startTable, no need to table to find it from.
				nc.reset(&t.title_principals);
				break;
			case Table::title_principals:
				tc.reset(); // Included in startTable...
				nc.reset(); // Included in startTable...
				break;
			case Table::name_basics:
				tc.reset(&t.title_principals);
				nc.reset(); // Included in startTable...
				t.name_basics.parent = nullptr;
				t.ratings.parent = t.title_basics.parent = t.title_akas.parent = &t.title_principals;
				break;
			default:
				throw std::logic_error("initTablesAndColumns: Invalid startUpTable");
			}

			// Then apply used columns to the tables.
			//
			for (auto& node : ratings.m_columnInfos) {
				ColumnInfo& ci = node.second;
				if (ci.usedInQuery) {
					for (TableInfo* ti : ci.tables) {
						for (TableInfo* cur = ti; cur != nullptr; cur = cur->parent) {
							cur->used = true;
						}
					}
				}
			}
		}

		void addAuxTablesRaw(Table startTable = Table::ratings, unsigned indentSize = 0)
		{
			ratings.getTableInfo(startTable).included = true; // Do this here so it's done also if addAuxTablesMultipleCalls is called.
			std::string const indent(indentSize, ' ');

			auto includeImpl = [&](TableInfo& table, const char* join, const char* sql) {
				if (table.used && !table.included) {
					add(indent); a(join); a(" "); a(sql);
					table.included = true;
				}
			};
			auto include = [&](TableInfo& table, const char* sql) {
				includeImpl(table, "JOIN", sql);
			};
			auto includeLeft = [&](TableInfo& table, const char* sql) {
				includeImpl(table, "LEFT JOIN", sql);
			};
			auto includeLeftIf = [&](TableInfo& table, const char* sql, Table leftJoinUnlessThis) {
				includeImpl(table, startTable == leftJoinUnlessThis ? "JOIN" : "LEFT JOIN", sql);
			};

			auto& t = ratings.m_tableInfos;

			switch (startTable) {
			case Table::ratings:
			case Table::title_basics:
			case Table::title_akas:
				include(t.title_principals, "title_principals USING(tconst)");
				break;
			case Table::title_principals:
				break;
			case Table::name_basics:
				include(t.title_principals, "title_principals USING(nconst)");
				break;
			}

			include(t.ratings, "ratings USING(tconst)");
			include(t.title_basics, "title_basics USING(tconst)");
			include(t.title_akas, "title_akas USING(tconst)");
			include(t.name_basics, "name_basics USING(nconst)");
		}

		void addAuxTables(Table startTable = Table::ratings, unsigned indentSize = 0)
		{
			initTablesAndColumns(startTable);
			addAuxTablesRaw(startTable, indentSize);
			ratings.resetTablesAndColumns();
		}

		void addAuxTablesMultipleCalls(Table startTable = Table::ratings, unsigned indentSize = 0)
		{
			addAuxTablesRaw(startTable, indentSize);
			for (auto& ti : ratings.m_tableInfos.arrView) ti.included = false;
		}

		void addOrderBy()
		{
			if (!m_orderBy.empty()) {
				m_query.append("\nORDER BY ");
				for (unsigned i = 0; i < m_orderBy.size(); ++i) {
					if (i != 0) m_query.append(",");
					auto ci = m_orderBy[i].first;
					auto order = (ColumnSortOrder)m_orderBy[i].second;
					// Use label if column is used in result(select), otherwise, have to use the name/def.
					// For window function based columns the latter may cause an out of memory error for some reason!
					// But only if the column is also included in the result, if the column is only used in order by it works!
					m_query.append(ci->usedInResult ? ci->labelName() : ci->nameDef);
					if (ci->collation != nullptr) m_query.append(" COLLATE ").append(ci->collation);
					if (order == ColumnSortOrder::Desc) m_query.append(" DESC"); // ASC is default.
				}
			}
			if (ratings.m_limit > 0) {
				m_query.append("\nLIMIT ").append(toStr(ratings.m_limit));
				if (ratings.m_offset > 0) m_query.append(" OFFSET ").append(toStr(ratings.m_offset));
			}

			// Needed when called multiple times in same LITT session (like when listing items for book input!)
			for (auto& node : ratings.m_columnInfos) node.second.usedInResult = false;
		}
	}; // OutputQuery

	static const char* rowValue(const char* val)  // Guard against nullptr:s coming from NULL db values.
	{
		return (val != nullptr) ? val : "";
	}

	void outputRow(OutputQuery const& query, bool isHeader, int argc, char** argv) const
	{
		switch (m_displayMode) {
		case DisplayMode::column:
			if (m_ansiEnabled) ansiSetRowColors(isHeader, argc, argv);
			break;
		case DisplayMode::htmldoc:
		case DisplayMode::html:
			m_output.write("<tr>");
			break;
		}

		for (int i = 0; i < argc; i++) {
			auto val = rowValue(argv[i]);
			switch (m_displayMode) {
			case DisplayMode::column:
				if ((size_t)i == query.columnSettings.size()) {
					// This will happen if "execute" is used to execute two (or more) different SQL 
					// queries where the latter one contains more columns that the former. 
					// Just add a suitable value to avoid crash. No need to support this use case further.
					query.columnSettings.emplace_back(30, JLeft);
				}
				if (auto cs = query.columnSettings[i]; cs.width > 0) {
					if (i != 0) m_output.write(m_colSep);
					if (m_ansiEnabled && m_ansiRowColors[i].get() != m_ansiDefColor) m_output.write(m_ansiRowColors[i].get());
					m_output.writeUtf8Width(val, cs.width, cs.justify);
					if (m_ansiEnabled && m_ansiRowColors[i].get() != m_ansiDefColor) m_output.write(m_ansiDefColor);
				}
				break;
			case DisplayMode::list:
				if (i != 0) m_output.write(m_listSep);
				m_output.write(val);
				break;
			case DisplayMode::tabs:
				if (i != 0) m_output.write('\t');
				m_output.write(val);
				break;
			case DisplayMode::html:
			case DisplayMode::htmldoc:
				m_output.write(isHeader ? "<th>" : "<td>");
				m_output.writeHtml(val);
				m_output.write(isHeader ? "</th>" : "</td>");
				m_output.write('\n');
				break;
			}
		}

		switch (m_displayMode) {
		case DisplayMode::htmldoc:
		case DisplayMode::html:
			m_output.write("</tr>");
			break;
		}
		m_output.write('\n');
	}

	struct AnsiColumnColor {
		std::string colName;
		std::string ansiColor;
	};

	struct AnsiValueColor {
		std::string colName; // For this column,
		std::regex  rowValueRegEx; // matching this row value,
		std::string ansiColor; // apply this row color,
		std::vector<std::string> coloredColumns; // to these columns.
	};

	struct AnsiValueColorIndexed {
		int colIndex;
		std::regex  rowValueRegEx;
		std::string ansiColor;
		std::vector<int> colIndexes;
	};

	bool m_ansiEnabled = false;
	std::string m_ansiDefColor = "\x1b[0m";

	std::vector<AnsiColumnColor> m_ansiColColors;
	mutable std::vector<std::string> m_ansiColColorsIndexed;

	std::vector<AnsiValueColor> m_ansiValueColors;
	mutable std::vector<AnsiValueColorIndexed> m_ansiValueColorsIndexed;

	mutable std::vector<std::reference_wrapper<std::string const>> m_ansiRowColors;

	void ansiInit(int argc, char** azColName) const
	{
		enableVTMode();
		m_output.write(m_ansiDefColor);

		for (int i = 0; i < argc; ++i) {
			m_ansiRowColors.emplace_back(m_ansiDefColor); // Just to init the size, will need to reset for each row.
			m_ansiColColorsIndexed.push_back(m_ansiDefColor); // Init to defaults
		}

		for (auto const& acc : m_ansiColColors) {
			for (int i = 0; i < argc; ++i) {
				if (acc.colName == azColName[i]) {
					m_ansiColColorsIndexed[i] = acc.ansiColor;
					break;
				}
			}
		}

		for (auto& avc : m_ansiValueColors) {
			for (int i = 0; i < argc; ++i) {
				if (avc.colName == azColName[i]) {
					AnsiValueColorIndexed indexed;
					indexed.colIndex = i;
					indexed.rowValueRegEx = avc.rowValueRegEx;
					indexed.ansiColor = avc.ansiColor;
					for (auto const& cc : avc.coloredColumns) {
						for (int ii = 0; ii < argc; ++ii) {
							if (cc == azColName[ii]) {
								indexed.colIndexes.push_back(ii);
								break;
							}
						}
					}
					if (!indexed.colIndexes.empty())
						m_ansiValueColorsIndexed.push_back(indexed);
					break;
				}
			}
		}
	}

	void ansiSetRowColors(bool /*isHeader*/, int argc, char** argv) const
	{
		for (int i = 0; i < argc; ++i) m_ansiRowColors[i] = m_ansiColColorsIndexed[i];

		for (auto const& avc : m_ansiValueColorsIndexed) {
			auto val = rowValue(argv[avc.colIndex]);
			if (std::regex_search(val, avc.rowValueRegEx))
				for (auto index : avc.colIndexes) m_ansiRowColors[index] = avc.ansiColor;
		}
	}

	enum class ConsRowMatchMethod {
		columnValue,
		regEx,
		regExNot,
	};

	struct ConsRowColumnInfo {
		std::string        name;
		ConsRowMatchMethod matchMethod;
		int                charCmpCount;
		std::regex         re;
		int                diff;
		mutable int        index;
	};

	int m_consRowMinCount = 0;
	std::vector<ConsRowColumnInfo> m_consRowColumns;
	mutable std::vector<std::vector<std::string>> m_consRowBuffer;
	mutable int m_consMatched = 0;

	bool consEnabled() const { return m_explainQuery == ExQ::None && m_consRowMinCount > 0; }

	void consInit(int argc, char** argv, char** azColName) const
	{
		_ASSERT(consEnabled());
		m_consRowBuffer.resize(std::max(1, m_consRowMinCount - 1));
		for (auto& row : m_consRowBuffer) row.resize(argc);

		for (auto& col : m_consRowColumns) {
			int j = 0;
			for (; j < argc; ++j) {
				if (col.name == azColName[j]) {
					col.index = j;
					break;
				}
			}
			if (j == argc)
				throw std::invalid_argument("Could not find cons column " + col.name + " in output columns");
		}
		consSetBufferRow(0, argv);
		m_consMatched = 0;
	}

	void consSetBufferRow(int index, char** argv) const
	{
		std::transform(argv, argv + m_consRowBuffer[index].size(), m_consRowBuffer[index].begin(), rowValue);
	}

	void consOutputMatchedCount() const // OBS! This is mainly intended for use with column display mode.
	{
		if (m_consMatched >= m_consRowMinCount)
			m_output.write(fmt("\n# = %i\n\n", m_consMatched));
	}

	void consProcessRow(OutputQuery const& query, int argc, char** argv) const
	{
		_ASSERT(consEnabled());
		bool cvMatch = true, reMatch = true;
		for (auto const& col : m_consRowColumns) {
			auto val = rowValue(argv[col.index]);
			switch (col.matchMethod) {
			case ConsRowMatchMethod::columnValue:
				if (cvMatch) {
					auto const& prevVal = m_consRowBuffer[0][col.index];
					if (col.charCmpCount <= 0) {
						cvMatch = (prevVal == val);
					}
					else {
						auto const maxCmp = (size_t)col.charCmpCount;
						auto const prevLen = prevVal.size();
						auto const len = strlen(val);
						if (len != prevLen && (len < maxCmp || prevLen < maxCmp)) {
							// If one (or both) of them is less than maxCmp and they have different lengths, then they are not equal.
							cvMatch = false;
						}
						else {
							auto const cmpCnt = std::min({ maxCmp, prevLen, len });
							for (size_t i = 0; i < cmpCnt; ++i) {
								if (prevVal[i] != val[i]) {
									cvMatch = false;
									break;
								}
							}
						}
					}
				}
				break;
			case ConsRowMatchMethod::regEx:
				reMatch = reMatch && std::regex_search(val, col.re);
				break;
			case ConsRowMatchMethod::regExNot:
				reMatch = reMatch && !std::regex_search(val, col.re);
				break;
			}
		}
		bool const consMatch = cvMatch && reMatch;

		if (consMatch) {
			++m_consMatched;
			if (m_consMatched == m_consRowMinCount && !(m_consRowMinCount == 1)) {
				std::vector<char*> rChar(m_consRowBuffer[0].size());
				for (auto const& r : m_consRowBuffer) {
					std::transform(r.begin(), r.end(), rChar.begin(),
						[](std::string const& str) { return const_cast<char*>(str.c_str()); });
					outputRow(query, false, rChar.size(), rChar.data());
				}
			}
			consSetBufferRow(std::min(m_consMatched - 1, std::max(0, m_consRowMinCount - 2)), argv);
			if (m_consMatched >= m_consRowMinCount)
				outputRow(query, false, argc, argv);
		}
		else {
			consOutputMatchedCount();
			consSetBufferRow(0, argv);
			m_consMatched = reMatch ? 1 : 0;
			if (m_consRowMinCount == 1 && reMatch)
				outputRow(query, false, argc, argv);
		}
	}

	struct EQPGraphRow {
		int iEqpId;        /* ID for this row */
		int iParentId;     /* ID of the parent row */
		std::string text;  /* Text to display for this row */
	};

	class EQPGraph {
		Output const& m_output;
		std::vector<EQPGraphRow> rows;
		std::string prefix;

		bool indexValid(int index) const { return 0 <= index && index < (int)rows.size(); }

		int nextRowIndex(int iEqpId, int oldIndex)
		{
			int row = indexValid(oldIndex) ? (oldIndex + 1) : 0;
			while (indexValid(row) && rows[row].iParentId != iEqpId) ++row;
			return row;
		}

		void renderLevel(int iEqpId) {
			int next = -1;
			for (int row = nextRowIndex(iEqpId, -1); indexValid(row); row = next) {
				next = nextRowIndex(iEqpId, row);
				m_output.write(prefix);
				m_output.write(indexValid(next) ? "|--" : "`--");
				m_output.write(rows[row].text);
				m_output.write("\n");
				prefix += (indexValid(next) ? "|  " : "   "); // len = 3
				renderLevel(rows[row].iEqpId);
				prefix.resize(prefix.size() - 3);
			}
		}
	public:
		EQPGraph(Output const& output) :m_output(output) {}

		void appendRow(int iEqpId, int parentId, const char* zText)
		{
			if (!rows.empty() && rows.back().iEqpId > iEqpId) { // => New query starts
				render();
				rows.clear();
			}
			rows.push_back(EQPGraphRow{ iEqpId, parentId, zText });
		}

		void render()
		{
			if (rows.empty()) return;
			m_output.write("QUERY PLAN\n");
			prefix.clear();
			renderLevel(0);
		}
	};

	mutable std::unique_ptr<EQPGraph> m_eqpGraph;

	void writeBomIfNeeded() const
	{
		static bool wroteBom = false;
		if (wroteBom) return;

		if (m_displayMode == DisplayMode::column) {
			// HACK: Write the UTF-8 BOM, seems V/VIEW needs it to properly 
			// detect the utf-8 encoding depending on the actual output.
			// Seems to interfere with V:s CSV mode though!
			m_output.writeUtf8Bom();
			wroteBom = true;
		}
	}

	int outputCallBack(OutputQuery const& query, int argc, char** argv, char** azColName)
	{
		try {
			if (m_rowCount == 0) {
				writeBomIfNeeded();

				if (m_explainQuery == ExQ::Graph) {
					m_eqpGraph = std::make_unique<EQPGraph>(m_output);
				}
				else if (m_displayMode == DisplayMode::column) {
					if (m_explainQuery != ExQ::None) { // Override column settings for explain output.
						if (m_explainQuery == ExQ::VMCode)   query.columnSettings = { 5, 20, 6, 6, 6, 30, 6, 0/*comment, not in LITT*/ };
						else if (m_explainQuery == ExQ::Raw) query.columnSettings = { 5/*id*/, 6/*parent*/, 0/*not used*/, 100/*detail*/ };
					}

					for (int i = query.columnSettings.size(); i < argc; ++i) { // Add missing columnSettings (for execute action).
						query.columnSettings.emplace_back(std::max(strlen(azColName[i]), strlen(rowValue(argv[i]))), JLeft);
						for (auto const& e : m_columnInfos) { // Try to find CS from columnInfos
							if (auto ci = e.second; azColName[i] == ci.label || azColName[i] == ci.nameDef ||
								azColName[i] == ci.nameDef.substr(1, ci.nameDef.length() - 2)) {
								query.columnSettings.back() = ColumnSetting(std::max(ci.width, query.columnSettings.back().width), ci.justify);
								break;
							}
						}
					}

					bool fitW = (m_fitWidth == FitWidth::on);
					bool const autoFit = (m_fitWidth == FitWidth::automatic && m_output.stdOutIsConsole());
					if (fitW || autoFit) {
						int requiredWidth = 0;
						for (auto& cs : query.columnSettings) {
							if (cs.width > m_fitWidthValue) cs.width = m_fitWidthValue; // Guard against HUGE sizes, can be slow to inc!
							requiredWidth += cs.width;
						}
						requiredWidth += (m_colSepSize * (query.columnSettings.size() - 1));
						if (autoFit) fitW = (requiredWidth > m_fitWidthValue);

						if (fitW) {
							int const target = m_fitWidthValue;
							int const current = requiredWidth;
							if (current != target) {
								int const inc = (target > current) ? 1 : -1;
								int diff = abs(target - current);
								for (;;) {
									int changed = 0;
									for (auto& cs : query.columnSettings) {
										// Don't touch columns with smallish widths (IDs, dates)
										// Also don't make arbitrarily small and large. 
										// Currently widest value in any column (not ng!) is 59.
										if (10 < cs.width && (inc < 0 || cs.width < 60)) {
											cs.width += inc;
											changed += abs(inc);
											if (changed >= diff) goto done;
										}
									}
									if (changed == 0) goto done;
									diff -= changed;
								}
							done:;
							}
						}
					}

					if (m_ansiEnabled) ansiInit(argc, azColName);
				}
				else if (m_displayMode == DisplayMode::htmldoc) {
					auto docStart =
						"<!DOCTYPE html>\n"
						"<html>\n"
						"<head>\n"
						"<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />\n"
						"<title>" + m_action + "</title>\n"
						"</head>\n"
						"<body>\n"
						"<table>\n";
					m_output.write(docStart);
				}

				if (!m_eqpGraph) {
					if (m_headerOn) {
						outputRow(query, true, argc, azColName);
						if (m_displayMode == DisplayMode::column) {
							for (int i = 0; i < argc; ++i) {
								if (auto w = query.columnSettings[i].width; w > 0) {
									if (i != 0) m_output.write(m_colSep);
									m_output.write(std::string(w, '-'));
								}
							}
							m_output.write('\n');
						}
					}
					if (consEnabled()) consInit(argc, argv, azColName);
				}
			} // if (m_rowCount == 0) {

			if (m_eqpGraph && argc == 4) {
				m_eqpGraph->appendRow(atoi(argv[0]), atoi(argv[1]), argv[3]);
			}
			else {
				if (consEnabled())
					consProcessRow(query, argc, argv);
				else
					outputRow(query, false, argc, argv);
			}
			++m_rowCount;
			return 0;
		}
		catch (std::exception& ex) {
			if (!m_output.error()) {
				m_output.flushNoThrow();
				fprintf(stderr, "\nCallback exception: %s\n", ex.what());
			}
			return 1;
		}
	}

	void runOutputQuery(OutputQuery const& q)
	{
		m_rowCount = 0;
		std::string sql = encodeSqlFromInput(q.m_query);

		if (m_showQuery) {
			m_output.write(sql); m_output.write('\n'); m_output.flush();
			return;
		}
		auto cb = [](void* pArg, int argc, char** argv, char** azColName) {
			auto q = static_cast<OutputQuery const*>(pArg);
			return q->ratings.outputCallBack(*q, argc, argv, azColName);
		};
		if (sqlite3_exec(m_conn.get(), sql.c_str(), cb, &const_cast<OutputQuery&>(q), nullptr) == SQLITE_OK) {
			if (m_eqpGraph) {
				m_eqpGraph->render();
				m_eqpGraph.reset();
			}
			else {
				if (m_displayMode == DisplayMode::htmldoc) m_output.write("</table>\n</body>\n</html>\n");
				if (consEnabled()) consOutputMatchedCount(); // In case matching was still ongoing at the last row.
			}
			m_output.flush();
			if (m_showNumberOfRows) printf("\n# = %i\n", m_rowCount);
		}
		else {
			if (!m_output.error()) // Don't throw new error if output already generated one (and caused exec failure).
				throw std::runtime_error(fmt("%s\n\nSQL error: %s", S(sql), sqlite3_errmsg(m_conn.get())));
		}
	}

	void runStandardOutputQuery(OutputQuery& query, Table startTable = Table::ratings)
	{
		query.addAuxTables(startTable);
		query.addWhere();
		query.addOrderBy();
		runOutputQuery(query);
	}

	void runListData(const char* defColumns, const char* defOrderBy, Table startTable = Table::ratings, SelectOption selOpt = SelectOption::normal)
	{
		OutputQuery query(*this, defColumns, getTableName(startTable), defOrderBy, selOpt);
		runStandardOutputQuery(query);
	}

	void listRatings(std::string const& title)
	{
		addActionWhereCondition("ti", title);
		runListData("ti.dr.ra.ty.ye.url", "dr.ti");
	}

	void listRerated()
	{
		auto from = "(SELECT tconst, Count(tconst) As Count FROM ratings GROUP BY tconst HAVING Count(tconst) > 1)";
		OutputQuery query(*this, "ti.dr.ra.ty.ye.url", from, "ti.dr");
		query.add("JOIN Ratings USING(tconst)");
		runStandardOutputQuery(query);
	}

	void listSametitle()
	{
		auto from = "(SELECT Title, Count(Title) As Count FROM (SELECT DISTINCT tconst, Title FROM ratings) GROUP BY Title HAVING Count(Title) > 1)";
		OutputQuery query(*this, "tiye.dr.ra.ty.url", from, "ti.ye.dr");
		query.add("JOIN Ratings USING(Title)");
		runStandardOutputQuery(query);
	}

	void listRatingCounts(std::string const& listBy, std::string const& countCond)
	{
		std::string cc = "rc";
		auto selCols = listBy + std::string(".") + cc;
		if (!countCond.empty()) appendToHaving(LogOp_AND, parseCountCondition(getColumn(cc)->nameDef, countCond));
		getColumn(listBy)->usedInQuery = true;

		OutputQuery query(*this, selCols.c_str(), "ratings", (cc + ".desc").c_str());
		query.addWhere();
		query.add("GROUP BY " + getColumn(listBy)->nameDef);
		query.addHaving();
		query.addOrderBy();
		runOutputQuery(query);
	}

	int executeSql(std::string const& sql, int (*cb)(void*, int, char**, char**) = nullptr, void* cbArg = nullptr, bool enableShowQuery = true) const
	{
		auto encSql = encodeSqlFromInput(sql);
		if (enableShowQuery && m_showQuery) {
			m_output.write(encSql); m_output.write('\n'); m_output.flush();
			return 0;
		}

		if (sqlite3_exec(m_conn.get(), encSql.c_str(), cb, cbArg, nullptr) == SQLITE_OK)
			return sqlite3_changes(m_conn.get());
		else
			throw std::runtime_error(fmt("SQL error: %s", sqlite3_errmsg(m_conn.get())));
	}

	void explainNotSupported()
	{
		if (m_explainQuery != ExQ::None)
			throw std::invalid_argument("-x option is not supported for this action");
	}

	constexpr static unsigned short actionHash(const char* action)
	{
		unsigned short h = 0; while (*action) h = 23 * (h ^ (unsigned short)(*action++));
		return h;
	}

	void showHelp(int level = 0)
	{
		fputs(
			R"(Usage: RATINGS {options} <action with arguments> {options}

Actions:
   h[0..1]        Show help, level 0..1, level 1 is default.
   r [title]      List rated movies

   re             List re-rated movies. Can use virtual column "rc" - rating count.
   st             List movies with same title. Can use virtual column "tc" - title count.
   rc <col> [rcc] List rating counts for column. Can use virtual column rc. rcc = rating count condition.

)", stdout); if (1 <= level) {
			fputs(
				R"(
NOTE: As wildcards in most match arguments and options "*" (any string) and "_" (any character) can be used. Wild-cards "*" 
      around the listing actions also gives a similar effect, e.g. *b* will list all books containing the given title 
      string, while b* will only list books starting with it instead.
      
Options:
    -d[DisplayMode]   Display mode. Default is column.
    -h[on|off]        Header row on/off. Default is on.
    -c[selColumns]    Override the default columns of the action.
    -a[addColumns]    Include additional columns.
    -i[addColumns]    Include additional columns AND only list rows where they are not null.
    -o[colOrder]      Override sort order. By default sorts by used (-c or default) columns starting from left.
    -w[whereCond]     Add a WHERE condition - will be AND:ed with the one specified by the action and arguments.
                      If several -w options are included their values will be OR:ed together.
    -s[colSizes]      Override the default column sizes.
    -q[d]             Use debug mode - dump the SQL query/command instead of executing it. 
                      Adding 'd' also lists default columns for the action.
    -u                Make sure the result only contain DISTINCT (UNIQUE) values.
    -l[dbPath]        Specify ratings-sqlite database file. Uses "ratings.sqlite" by default. Either from the
                      current directory or from "%MYDOCS%\ratings\" if MYDOCS is set.
    -n                Print number of output rows at the end.
    -e[encoding]      Output encoding for pipes and redirection. Default is utf8.
    -f[on|off|auto|w] Fit column width mode. Default is auto => fit only when console is too narrow.
                      Specifying an explicit width value implies mode "on". If no value is specified then 
                      the width of the console is used. If there is no console then a hard-coded value is used.
    -x[1|2|3]         Explains the query plan. x/x1 = graph EQP output, x2 = VM code output, x3 = raw EQP output.

    --cons:<minRowCount>:{<colSnOrName>[:charCmpCount]|[:re|ren:<regExValue>]}+
                     Specify column conditions for consecutive output row matching.
                     If no explicit method is specified then matching is done by comparing against the
                     same column value of the previous row. Can optionally specify the max number of characters
                     to compare.

    --ansi[:off][:defC:<ansiC>][:colC:<col>:<ansiC>][:valC:<colVal>:<regExValue>:col{.col}:<ansiC>}
                     Specify ANSI colors for columns, rows and specific values. Only enabled in column display mode.
                     * off  : Turn off ANSI coloring. Default is on when --ansi option is included.
                     * defC : Specify default color for all values.
                     * colC : Specify ANSI color for given column (either given as short name or full name).
                     * valC : Specify ANSI color for the given columns when the value of the given value
                              columns matches the included regex.

    --limit:<n>      LIMIT value.
    --offset:<n>     OFFSET value.

    --colsep:<str>   The column separator used in columns display mode. Default = "  "
    
    For escaping option separators the escape character '!' can be used. It's also used to escape itself.
    Note that if an option is included several times, then the last one will normally be the effective one.
    Some options like -a and -w are additive though and all option instances of those will be used.

selColumns format: <shortName>[.<width>]{.<shortName>[.<width>]}
addColumns format: Same as selColumns format.
colOrder format: <shortName|actualName>[.asc|desc]{.<shortName|actualName>[.asc|desc]}
whereCond format: <shortName>[.<cmpOper>].<cmpArg>{.<shortName>[.<cmpOper>].<cmpArg>}
          cmpOper: lt,lte,gt,gte,eq,neq,glob,nglob,isnull,notnull,isempty ("LIKE" if none is given, isnull, notnull & isempty take no cmpArg)
          cmpOper: range,nrange - These take two cmpArgs, for start and stop of range (both inclusive)
          cmpOper: and,or,nand,nor - These will consume the rest of the whereCond terms and AND/OR/NAND/NOR them using LIKE/=.
colSizes format: Same as selColumns format

rating count condition formats:
    <number>        Only includes values with rating count >= <number>
    lt.<number>     Only includes values with rating count < <number>
    gt.<number>     Only includes values with rating count > <number>
    eq.<number>     Only includes values with rating count = <number>
    range.<n1>.<n2> Only includes Only includes values with rating count in range [n1,n2]

DisplayMode values:
    col|column  Left-aligned columns (Specified or default widths are used)
    html        HTML table code
    htmldoc     Full HTML document
    list[:sep]  Values delimited by separator string, default is "|"
    tabs        Tab-separated values

Column short name values:
)", stdout);
			for (auto const& c : m_columnInfos) if (!c.second.nameDef._Starts_with("length("))
				printf("    %-10s - %s\n", c.first.c_str(), unquote(c.second.labelName()).c_str());
			fputs(R"(
    To get the length of column values "l" can be appended to the short name for non-numeric/ID columns.
    E.g. "gel" will provide the lengths of the "ge" column values.
)", stdout);
		}
	}

	void executeAction()
	{
		constexpr auto a = actionHash;
		switch (auto const& act = m_action; a(act.c_str())) {
		case a(""): case a("h"): case a("h0"): case a("h1"): showHelp(act == "h" ? 1 : act[1] - '0'); break;
		case a("r"):  listRatings(arg(0)); break;
		case a("re"): listRerated(); break;
		case a("st"): listSametitle(); break;
		case a("rc"): listRatingCounts(arg(0), arg(1)); break;
		default:
			throw std::invalid_argument("Invalid action: " + act);
		}
	}
}; // Ratings

int main(int argc, char** argv)
try {
	Ratings(argc, argv).executeAction();
}
catch (std::exception& ex) {
	fprintf(stderr, "%s\n", ex.what());
	return 1;
}
