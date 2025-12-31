#!/usr/bin/env node
/**
 * SEO PDF Sync Script - Enhanced Version
 *
 * Features:
 * - OAuth token refresh handling
 * - Bad PDF detection and filtering
 * - Archive processed files to Drive
 * - Processing manifest to track state
 * - Incremental sync (only new files)
 *
 * Usage:
 *   node sync-seo-pdfs.js              # Normal sync
 *   node sync-seo-pdfs.js --force      # Re-download all
 *   node sync-seo-pdfs.js --archive    # Move processed to archive after sync
 *   node sync-seo-pdfs.js --validate   # Only validate existing PDFs
 */

import { google } from 'googleapis';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import { execSync } from 'child_process';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '.env') });

// Configuration
const CONFIG = {
  OUTPUT_DIR: '/Users/anthonyrobinson/booteek/seo-insights',
  MANIFEST_FILE: '/Users/anthonyrobinson/booteek/seo-insights/processing-manifest.json',
  TOKEN_FILE: path.join(__dirname, 'token.json'),
  MIN_PDF_SIZE: 1024,           // 1KB minimum (smaller = likely error page)
  MAX_PDF_SIZE: 50 * 1024 * 1024, // 50MB maximum
  ARCHIVE_FOLDER_NAME: '_Processed',

  // Source folders
  FOLDERS: {
    'AEO-GEO': '1GZTXiusU5pmR3z920WT3L_nhNOKoAxcy',
    'ThreadReaderApp': '1oV3S12moAxhvVQEX1yX_IHoMIUYmythc'
  }
};

// Parse command line args
const args = process.argv.slice(2);
const FORCE_DOWNLOAD = args.includes('--force');
const ARCHIVE_AFTER = args.includes('--archive');
const VALIDATE_ONLY = args.includes('--validate');

let oauth2Client;
let drive;

/**
 * Initialize OAuth client with token refresh
 */
async function initializeAuth() {
  oauth2Client = new google.auth.OAuth2(
    process.env.CLAUDE_CODE_GD_CLIENT_ID,
    process.env.CLAUDE_CODE_GD_CLIENT_SECRET,
    'http://localhost:3000/oauth2callback'
  );

  // Load existing token
  if (!fs.existsSync(CONFIG.TOKEN_FILE)) {
    console.error('\n[ERROR] No token.json found!');
    console.error('Run: npm run tokenGenerator');
    console.error('Then authenticate in browser at http://localhost:3000\n');
    process.exit(1);
  }

  const token = JSON.parse(fs.readFileSync(CONFIG.TOKEN_FILE, 'utf8'));
  oauth2Client.setCredentials(token);

  // Set up automatic token refresh
  oauth2Client.on('tokens', (newTokens) => {
    console.log('[AUTH] Token refreshed automatically');

    // Merge new tokens with existing (preserve refresh_token if not in new)
    const updatedToken = { ...token, ...newTokens };
    fs.writeFileSync(CONFIG.TOKEN_FILE, JSON.stringify(updatedToken, null, 2));
  });

  // Test auth and force refresh if needed
  try {
    await testAuth();
    console.log('[AUTH] Authentication valid\n');
  } catch (err) {
    if (err.message.includes('invalid_grant') || err.message.includes('Token has been expired')) {
      console.error('\n[AUTH] Token expired and cannot be refreshed.');
      console.error('Please re-authenticate:');
      console.error('  1. Run: npm run tokenGenerator');
      console.error('  2. Open http://localhost:3000 in browser');
      console.error('  3. Complete OAuth flow\n');
      process.exit(1);
    }
    throw err;
  }

  drive = google.drive({ version: 'v3', auth: oauth2Client });
}

/**
 * Test authentication by making a simple API call
 */
async function testAuth() {
  const testDrive = google.drive({ version: 'v3', auth: oauth2Client });
  await testDrive.about.get({ fields: 'user' });
}

/**
 * Load or create processing manifest
 */
function loadManifest() {
  if (fs.existsSync(CONFIG.MANIFEST_FILE)) {
    return JSON.parse(fs.readFileSync(CONFIG.MANIFEST_FILE, 'utf8'));
  }
  return {
    lastSync: null,
    processed: {},      // fileId -> { name, processedAt, status, localPath }
    errors: {},         // fileId -> { name, error, lastAttempt }
    archived: {}        // fileId -> { name, archivedAt }
  };
}

/**
 * Save processing manifest
 */
function saveManifest(manifest) {
  manifest.lastSync = new Date().toISOString();
  fs.writeFileSync(CONFIG.MANIFEST_FILE, JSON.stringify(manifest, null, 2));
}

/**
 * Validate a downloaded PDF
 */
function validatePdf(filePath, fileName) {
  const errors = [];

  // Check file exists
  if (!fs.existsSync(filePath)) {
    return { valid: false, errors: ['File does not exist'] };
  }

  const stats = fs.statSync(filePath);

  // Check file size
  if (stats.size < CONFIG.MIN_PDF_SIZE) {
    errors.push(`File too small (${stats.size} bytes) - likely error page`);
  }
  if (stats.size > CONFIG.MAX_PDF_SIZE) {
    errors.push(`File too large (${(stats.size / 1024 / 1024).toFixed(1)} MB)`);
  }

  // Check PDF header
  const fd = fs.openSync(filePath, 'r');
  const header = Buffer.alloc(8);
  fs.readSync(fd, header, 0, 8, 0);
  fs.closeSync(fd);

  const headerStr = header.toString('utf8');
  if (!headerStr.startsWith('%PDF')) {
    errors.push(`Invalid PDF header: "${headerStr.substring(0, 4)}" (expected %PDF)`);

    // Check if it's HTML (login page)
    if (headerStr.includes('<!DO') || headerStr.includes('<htm') || headerStr.includes('<HTM')) {
      errors.push('File is HTML, not PDF (likely login redirect)');
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    size: stats.size,
    sizeFormatted: formatBytes(stats.size)
  };
}

/**
 * Format bytes to human readable
 */
function formatBytes(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / 1024 / 1024).toFixed(1) + ' MB';
}

/**
 * Download a single file with validation
 */
async function downloadFile(fileId, fileName, folderName, manifest) {
  const outputFolder = path.join(CONFIG.OUTPUT_DIR, folderName);
  if (!fs.existsSync(outputFolder)) {
    fs.mkdirSync(outputFolder, { recursive: true });
  }

  const outputPath = path.join(outputFolder, fileName);

  // Check if already processed successfully
  if (!FORCE_DOWNLOAD && manifest.processed[fileId]?.status === 'valid') {
    if (fs.existsSync(outputPath)) {
      console.log(`  [SKIP] ${fileName} (already processed)`);
      return { status: 'skipped', path: outputPath };
    }
  }

  // Check if in error list (skip unless forced)
  if (!FORCE_DOWNLOAD && manifest.errors[fileId]) {
    console.log(`  [SKIP] ${fileName} (previous error: ${manifest.errors[fileId].error})`);
    return { status: 'skipped_error', error: manifest.errors[fileId].error };
  }

  try {
    console.log(`  [DOWNLOAD] ${fileName}...`);

    const res = await drive.files.get(
      { fileId, alt: 'media' },
      { responseType: 'arraybuffer' }
    );

    fs.writeFileSync(outputPath, Buffer.from(res.data));

    // Validate the downloaded file
    const validation = validatePdf(outputPath, fileName);

    if (validation.valid) {
      console.log(`  [OK] ${fileName} (${validation.sizeFormatted})`);
      manifest.processed[fileId] = {
        name: fileName,
        processedAt: new Date().toISOString(),
        status: 'valid',
        localPath: outputPath,
        size: validation.size
      };
      // Clear any previous error
      delete manifest.errors[fileId];
      return { status: 'success', path: outputPath, size: validation.size };
    } else {
      console.log(`  [INVALID] ${fileName}: ${validation.errors.join(', ')}`);

      // Move invalid file to _invalid folder
      const invalidFolder = path.join(outputFolder, '_invalid');
      if (!fs.existsSync(invalidFolder)) {
        fs.mkdirSync(invalidFolder, { recursive: true });
      }
      const invalidPath = path.join(invalidFolder, fileName);
      fs.renameSync(outputPath, invalidPath);

      manifest.errors[fileId] = {
        name: fileName,
        error: validation.errors.join('; '),
        lastAttempt: new Date().toISOString(),
        movedTo: invalidPath
      };
      return { status: 'invalid', errors: validation.errors };
    }
  } catch (err) {
    console.error(`  [ERROR] ${fileName}: ${err.message}`);
    manifest.errors[fileId] = {
      name: fileName,
      error: err.message,
      lastAttempt: new Date().toISOString()
    };
    return { status: 'error', error: err.message };
  }
}

/**
 * Get or create archive folder in Drive
 */
async function getOrCreateArchiveFolder(parentFolderId) {
  // Check if archive folder exists
  const searchRes = await drive.files.list({
    q: `'${parentFolderId}' in parents and name='${CONFIG.ARCHIVE_FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder' and trashed=false`,
    fields: 'files(id, name)'
  });

  if (searchRes.data.files.length > 0) {
    return searchRes.data.files[0].id;
  }

  // Create archive folder
  const createRes = await drive.files.create({
    requestBody: {
      name: CONFIG.ARCHIVE_FOLDER_NAME,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [parentFolderId]
    },
    fields: 'id'
  });

  console.log(`  [ARCHIVE] Created ${CONFIG.ARCHIVE_FOLDER_NAME} folder`);
  return createRes.data.id;
}

/**
 * Move a file to archive folder in Drive
 */
async function archiveFile(fileId, fileName, sourceFolderId, manifest) {
  try {
    const archiveFolderId = await getOrCreateArchiveFolder(sourceFolderId);

    // Move file to archive (remove from parent, add to archive)
    await drive.files.update({
      fileId: fileId,
      addParents: archiveFolderId,
      removeParents: sourceFolderId,
      fields: 'id, parents'
    });

    manifest.archived[fileId] = {
      name: fileName,
      archivedAt: new Date().toISOString(),
      archiveFolderId
    };

    console.log(`  [ARCHIVED] ${fileName}`);
    return true;
  } catch (err) {
    console.error(`  [ARCHIVE ERROR] ${fileName}: ${err.message}`);
    return false;
  }
}

/**
 * Process a folder
 */
async function processFolder(folderName, folderId, manifest) {
  console.log(`\n${'='.repeat(50)}`);
  console.log(`FOLDER: ${folderName}`);
  console.log(`${'='.repeat(50)}`);

  const res = await drive.files.list({
    q: `'${folderId}' in parents and mimeType='application/pdf' and trashed=false`,
    fields: 'files(id, name, modifiedTime, size)',
    orderBy: 'modifiedTime desc',
    pageSize: 100
  });

  const files = res.data.files;
  console.log(`Found ${files.length} PDFs\n`);

  const stats = { success: 0, skipped: 0, invalid: 0, errors: 0 };
  const toArchive = [];

  for (const file of files) {
    if (VALIDATE_ONLY) {
      // Just validate existing local files
      const localPath = path.join(CONFIG.OUTPUT_DIR, folderName, file.name);
      if (fs.existsSync(localPath)) {
        const validation = validatePdf(localPath, file.name);
        if (validation.valid) {
          console.log(`  [VALID] ${file.name} (${validation.sizeFormatted})`);
          stats.success++;
        } else {
          console.log(`  [INVALID] ${file.name}: ${validation.errors.join(', ')}`);
          stats.invalid++;
        }
      } else {
        console.log(`  [MISSING] ${file.name}`);
        stats.errors++;
      }
    } else {
      const result = await downloadFile(file.id, file.name, folderName, manifest);

      switch (result.status) {
        case 'success':
          stats.success++;
          toArchive.push({ id: file.id, name: file.name });
          break;
        case 'skipped':
          stats.skipped++;
          // Already processed files can still be archived
          if (manifest.processed[file.id]?.status === 'valid') {
            toArchive.push({ id: file.id, name: file.name });
          }
          break;
        case 'skipped_error':
        case 'invalid':
          stats.invalid++;
          break;
        case 'error':
          stats.errors++;
          break;
      }
    }
  }

  // Archive successfully processed files if requested
  if (ARCHIVE_AFTER && toArchive.length > 0) {
    console.log(`\n  Archiving ${toArchive.length} processed files...`);
    for (const file of toArchive) {
      await archiveFile(file.id, file.name, folderId, manifest);
    }
  }

  return stats;
}

/**
 * Main execution
 */
async function main() {
  console.log('');
  console.log('╔══════════════════════════════════════════════════╗');
  console.log('║     SEO PDF Sync - Enhanced Version              ║');
  console.log('╠══════════════════════════════════════════════════╣');
  console.log(`║  Mode: ${VALIDATE_ONLY ? 'Validate Only' : FORCE_DOWNLOAD ? 'Force Download' : 'Incremental Sync'}`.padEnd(51) + '║');
  console.log(`║  Archive: ${ARCHIVE_AFTER ? 'Yes (move processed to Drive archive)' : 'No'}`.padEnd(51) + '║');
  console.log('╚══════════════════════════════════════════════════╝');

  // Initialize auth (with refresh handling)
  await initializeAuth();

  // Load manifest
  const manifest = loadManifest();
  if (manifest.lastSync) {
    console.log(`Last sync: ${new Date(manifest.lastSync).toLocaleString()}`);
  }

  // Process each folder
  const totalStats = { success: 0, skipped: 0, invalid: 0, errors: 0 };

  for (const [folderName, folderId] of Object.entries(CONFIG.FOLDERS)) {
    const stats = await processFolder(folderName, folderId, manifest);
    totalStats.success += stats.success;
    totalStats.skipped += stats.skipped;
    totalStats.invalid += stats.invalid;
    totalStats.errors += stats.errors;
  }

  // Save manifest
  if (!VALIDATE_ONLY) {
    saveManifest(manifest);
  }

  // Summary
  console.log('\n' + '='.repeat(50));
  console.log('SUMMARY');
  console.log('='.repeat(50));
  console.log(`  Success:  ${totalStats.success}`);
  console.log(`  Skipped:  ${totalStats.skipped}`);
  console.log(`  Invalid:  ${totalStats.invalid}`);
  console.log(`  Errors:   ${totalStats.errors}`);
  console.log(`  Total:    ${totalStats.success + totalStats.skipped + totalStats.invalid + totalStats.errors}`);

  if (Object.keys(manifest.errors).length > 0) {
    console.log('\nFiles with errors (use --force to retry):');
    for (const [id, info] of Object.entries(manifest.errors)) {
      console.log(`  - ${info.name}: ${info.error}`);
    }
  }

  console.log('\nDone!');
}

main().catch(err => {
  console.error('\n[FATAL ERROR]', err.message);
  if (err.message.includes('invalid_grant')) {
    console.error('\nToken expired. Run: npm run tokenGenerator');
  }
  process.exit(1);
});
