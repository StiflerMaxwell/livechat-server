// server.js
const express = require('express');
const multer = require('multer');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const SSE = require('express-sse');
require('dotenv').config(); // Load .env for local testing


const app = express();
const port = process.env.PORT || 3001;

// --- Directory Configuration ---
const baseDir = __dirname; // Get directory of server.js
const uploadDir = path.join(baseDir, 'uploads');
const outputDir = path.join(baseDir, 'analyzed_results');
const pythonScriptPath = path.join(baseDir, 'run_analysis_workflow.py');

// Get Python executable path
const pythonExecutablePath = process.env.PYTHON_EXECUTABLE || 'python';
if (pythonExecutablePath === 'python') {
    console.warn('PYTHON_EXECUTABLE environment variable is not set. Using "python" command.');
} else {
     console.log(`Using Python executable: ${pythonExecutablePath}`);
}


// Ensure directories exist
fs.existsSync(uploadDir) || fs.mkdirSync(uploadDir, { recursive: true });
fs.existsSync(outputDir) || fs.mkdirSync(outputDir, { recursive: true });

// --- Multer File Upload Configuration ---
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, uploadDir);
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = Date.now();
    // Use original base name + timestamp + original extension
    cb(null, `${path.parse(file.originalname).name}-${uniqueSuffix}${path.extname(file.originalname)}`);
  }
});
const upload = multer({ storage: storage, limits: { fileSize: 1024 * 1024 * 50 } }); // Limit file size to 50MB


// --- SSE Instance ---
const sse = new SSE();


// --- Middleware ---
 
const cors = require('cors');
// ...

// --- Define allowed origins ---
// Get your Netlify site's URL from the Netlify dashboard.
// It should look like https://your-site-name.netlify.app
// For local development, you might also need to allow localhost
const allowedOrigins = [
    'https://vertu-ga.netlify.app/', // <-- REPLACE with your actual Netlify site URL
    'http://localhost:5173', // Or your local frontend dev server URL
    'http://localhost:3000'  // Or your local frontend dev server URL
];

// --- CORS Configuration ---
app.use(cors({
  origin: function (origin, callback) {
    // Allow requests with no origin (like mobile apps or curl requests)
    // and requests from the defined allowedOrigins
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  }
  // You might also need to specify allowed methods and headers if you use non-default ones,
  // but for simple POST and GET with standard headers, origin is usually enough.
  // methods: ['GET', 'POST'],
  // allowedHeaders: ['Content-Type'],
}));


app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// --- Helper function to delete old analysis files ---
const cleanupOldAnalysisFiles = async () => {
    console.log(`Attempting to clean up old analysis files in: ${outputDir}`);
    try {
        const files = await fs.promises.readdir(outputDir);
        let deletedCount = 0;
        for (const file of files) {
            // Only delete files matching the analysis result pattern and with .xlsx extension
            if (file.startsWith('analysis_result_') && file.endsWith('.xlsx')) {
                 const filePath = path.join(outputDir, file);
                 try {
                     await fs.promises.unlink(filePath);
                     console.log(`Deleted old analysis file: ${filePath}`);
                     deletedCount++;
                 } catch (err) {
                     console.error(`Error deleting old analysis file ${filePath}:`, err);
                 }
            }
        }
        console.log(`Finished cleaning up old analysis files. Deleted ${deletedCount} files.`);
    } catch (err) {
         console.error(`Error reading analysis results directory for cleanup: ${outputDir}`, err);
    }
};
// --------------------------------------------------


// --- Routes ---

app.get('/', (req, res) => {
  res.send('Chat Analysis Backend is running!');
});

// SSE connection endpoint
app.get('/analysis_stream', sse.init);


// File upload and analysis initiation endpoint
app.post('/upload_and_analyze', upload.single('chatFile'), async (req, res) => { // Made route handler async
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded.' });
  }

  // --- NEW: Clean up old analysis files before starting new analysis ---
  await cleanupOldAnalysisFiles();
  // ---------------------------------------------------------------


  const inputJsonPath = req.file.path;
  const outputExcelFilename = `analysis_result_${Date.now()}_${path.parse(req.file.originalname).name}.xlsx`;
  const outputExcelPath = path.join(outputDir, outputExcelFilename); // Local path where Python saves


  const limit = req.body.limit;
  const limitArgs = [];
  if (limit !== undefined && limit !== null && limit !== '') {
      const limitNum = parseInt(limit, 10);
      if (!isNaN(limitNum) && limitNum >= 0) {
          limitArgs.push(limitNum.toString());
      } else {
          console.warn(`Received invalid limit value: ${limit}. Ignoring limit.`);
      }
  }

  const pythonArgs = [
    pythonScriptPath,
    inputJsonPath,       // Arg 1: Input JSON path (temporary file on Render)
    outputExcelPath,     // Arg 2: Output Excel path (local path on Render)
    ...limitArgs         // Arg 3 (optional): Limit value
  ];

  console.log(`Spawning Python process: ${pythonExecutablePath} ${pythonArgs.join(' ')}`);

  let pythonStdout = '';
  let pythonStderr = '';


  try {
      const pythonChildProcess = spawn(pythonExecutablePath, pythonArgs, {
          cwd: baseDir,
          env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONUNBUFFERED: '1' } // Ensure unbuffered output
      });

      // Capture and forward Python's stdout to SSE clients
      pythonChildProcess.stdout.on('data', (data) => {
        const dataString = data.toString();
        // console.log(`Python stdout: ${dataString.trim()}`); // Log to Node.js console
        pythonStdout += dataString;

        dataString.split('\n').forEach(line => {
            if (line.startsWith('PYTHON_STATUS:')) {
                sse.send({ type: 'status', message: line.substring('PYTHON_STATUS:'.length).trim() }, 'message');
            } else if (line.startsWith('PYTHON_PARSED_CHAT_RESULT:')) {
                 const jsonString = line.substring('PYTHON_PARSED_CHAT_RESULT:'.length).trim();
                 sse.send({ type: 'parsed_result', content: jsonString }, 'message');
            } else if (line.startsWith('PYTHON_RESULT_JSON:')) {
                 const jsonContent = line.substring('PYTHON_RESULT_JSON:'.length).trim();
                 try {
                     const finalResult = JSON.parse(jsonContent);
                     sse.send({ type: 'final_result_json', content: finalResult }, 'message');
                 } catch (e) {
                     console.error('Failed to parse final JSON result from stdout:', e);
                     sse.send({ type: 'log_error', message: `Failed to parse final JSON result: ${e}. Raw content: ${jsonContent.substring(0, 200)}...` }, 'message');
                 }
            } else {
                 if (line.trim()) {
                    sse.send({ type: 'log', message: line.trim() }, 'message');
                 }
            }
        });
      });

      // Capture and forward Python's stderr to SSE clients
      pythonChildProcess.stderr.on('data', (data) => {
        const dataString = data.toString();
        // console.error(`Python stderr: ${dataString.trim()}`); // Log to Node.js console
        pythonStderr += dataString;

         dataString.split('\n').forEach(line => {
             if (line.startsWith('PYTHON_FATAL_ERROR:')) {
                 sse.send({ type: 'fatal_error', message: line.substring('PYTHON_FATAL_ERROR:'.length).trim(), details: pythonStderr }, 'message');
             } else if (line.startsWith('PYTHON_ERROR:')) {
                  sse.send({ type: 'error', message: line.substring('PYTHON_ERROR:'.length).trim() }, 'message');
             } else if (line.startsWith('PYTHON_WARNING:')) {
                  sse.send({ type: 'warning', message: line.substring('PYTHON_WARNING:'.length).trim() }, 'message');
             } else if (line.trim()) {
                 sse.send({ type: 'log_error', message: line.trim() }, 'message');
             }
         });
      });

      // Listen for Python process exit
      pythonChildProcess.on('close', async (code) => { // Made the close listener async
        console.log(`Python process exited with code ${code}`);

        // Clean up temporary uploaded JSON file immediately after process finishes
        if (fs.existsSync(inputJsonPath)) {
             console.log(`Attempting to remove temporary input file: ${inputJsonPath}`);
             try {
                 await fs.promises.unlink(inputJsonPath); // Use async unlink
                 console.log(`Temporary input file ${inputJsonPath} removed.`);
             } catch (err) {
                  console.error(`Error removing temporary input file ${inputJsonPath}:`, err);
             }
        }

        // Use collected stdout/stderr for final error report if needed
        const finalStdout = pythonStdout;
        const finalStderr = pythonStderr;

        if (code === 0) {
          // Python script finished successfully
          // Check if the expected output file was created
          if (fs.existsSync(outputExcelPath)) {
            console.log(`Python generated output file: ${outputExcelPath}`);

            // --- Send 'complete' SSE event with the LOCAL download URL ---
            // Frontend will download this file from *this* Render backend instance
            sse.send({ // Data payload for the 'complete' event
              status: 'Analysis complete',
              filename: outputExcelFilename,
              // The frontend will use its backendUrl + this relative path to download
              downloadUrl: `/download/${outputExcelFilename}`,
              // Optional: Include captured output in complete event data for debugging
              // stdout: finalStdout,
              // stderr: finalStderr
            }, 'complete'); // <-- THIS is the SSE event name
            // -------------------------------------------------------------

            console.log(`Analysis complete. Local Download URL sent.`);

            // *** Removed immediate deletion here ***
            // The file will remain until the *next* upload triggers cleanupOldAnalysisFiles
            // or the Render instance restarts/cleans its ephemeral storage.


          } else {
            // Python exited with 0, but output file is missing - indicates a problem in Python's saving logic
            const errorMessage = 'Analysis completed (Python exit code 0), but output file was not found.';
            console.error(errorMessage);
             sse.send({
                 type: 'error', // Data payload type
                 message: errorMessage,
                 details: `Expected file: ${outputExcelPath}\n` + finalStdout + finalStderr
             }, 'error'); // Send as 'error' event
          }
        } else {
          // Python script failed (non-zero exit code)
          const errorMessage = `Analysis failed during Python execution. Exit code: ${code}.`;
          console.error(errorMessage);
           // Send an 'error' or 'fatal_error' event for process failure
           // Check if a fatal error was already reported by PythonStderr
           if (!finalStderr.includes('PYTHON_FATAL_ERROR:')) {
              sse.send({
                  type: 'fatal_error', // Use fatal_error for process termination
                  message: errorMessage,
                  details: finalStdout + finalStderr
              }, 'fatal_error');
           }
        }
      });

      // Listen for process spawn errors (e.g., python command not found)
      pythonChildProcess.on('error', async (err) => { // Made the error listener async
         const errorMessage = 'Failed to spawn python process. Check if python executable is in PATH or configured correctly via PYTHON_EXECUTABLE env var.';
         console.error(errorMessage, err);
         // Clean up input file in case of spawn error
         if (req.file && req.file.path && fs.existsSync(req.file.path)) {
             console.log(`Attempting to remove input file after spawn error: ${req.file.path}`);
             try {
                await fs.promises.unlink(req.file.path); // Use async unlink
                console.log(`Input file ${req.file.path} removed after spawn error.`);
             } catch (unlinkErr) {
                 console.error(`Error removing input file ${req.file.path} after spawn error:`, unlinkErr);
             }
         }
         // Send a fatal error event to the frontend
         sse.send({
             type: 'fatal_error',
             message: errorMessage,
             details: err.message
         }, 'fatal_error');
      });


      // Send initial success response to the frontend immediately after spawning
      res.status(202).json({
          status: 'File uploaded and analysis initiated. Connect to /analysis_stream for progress.',
          outputFilename: outputExcelFilename // Send the expected output filename (for frontend reference)
      });

  } catch (error) {
      // Catch synchronous errors during file upload or initial spawn setup
      console.error('Synchronous error during upload/spawn:', error);
      // Clean up input file in case of synchronous error
      if (req.file && req.file.path && fs.existsSync(req.file.path)) {
          console.log(`Attempting to remove input file after synchronous error: ${req.file.path}`);
          try {
             await fs.promises.unlink(req.file.path); // Use async unlink
             console.log(`Input file ${req.file.path} removed after synchronous error.`);
          } catch (unlinkErr) {
              console.error(`Error removing input file ${req.file.path} after synchronous error:`, unlinkErr);
          }
      }
      // Send error response to frontend
      res.status(500).json({ error: 'Server failed to initiate analysis.', details: error.message });
  }

});

// --- File download endpoint (serving from local disk) ---
app.get('/download/:filename', (req, res) => {
  const filename = req.params.filename;
  // Sanitize filename to prevent directory traversal attacks
  const safeFilename = path.basename(filename);
  const filePath = path.join(outputDir, safeFilename);

  console.log(`Attempting to serve local file for download: ${filePath}`);

  // Check if the file exists and is within the output directory
  // Note: File persistence here depends on Render's ephemeral storage and cleanup speed.
  if (fs.existsSync(filePath) && filePath.startsWith(outputDir)) {
    res.setHeader('Content-Disposition', `attachment; filename="${safeFilename}"`);
    // Use res.sendFile for serving files
    res.sendFile(filePath, (err) => {
      if (err) {
        console.error(`Error during file download ${filePath}:`, err);
        // Check if headers have already been sent before trying to send a new response
        if (!res.headersSent) {
             if (err.code === 'ENOENT') { // File not found error
                 res.status(404).send('File not found (might have been removed).');
             } else { // Other potential errors during transfer
                 res.status(500).send('Error downloading file.');
             }
        }
      } else {
          console.log(`File ${safeFilename} downloaded successfully.`);
          // File will be removed on the *next* file upload, not after download
      }
    });
  } else {
    console.warn(`File not found or not allowed for download: ${filePath}`);
    res.status(404).json({ error: 'File not found (might have been removed).' });
  }
});


// Start the Node.js server
app.listen(port, () => {
  console.log(`Node.js backend listening on port ${port}`);
  console.log(`GOOGLE_API_KEY loaded in Node.js: ${!!process.env.GOOGLE_API_KEY}`);
  // Verify Python script exists on startup
   if (!fs.existsSync(pythonScriptPath)) {
       console.error(`FATAL: Python workflow script not found at ${pythonScriptPath}. Analysis will fail.`);
   }
   console.log(`Using Python executable: ${pythonExecutablePath}`);
   // publicDownloadUrlBase is not used anymore for link construction, frontend uses its own backendUrl + relative path

});